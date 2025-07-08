"""Manage the API entrypoints"""

from collections.abc import Mapping
from typing import Any

from despsharedlibrary.schemas.collaborative_schema import AssetType
from elastic_transport import ConnectionError, HeadApiResponse
from elasticsearch import (
    AsyncElasticsearch,
    AuthenticationException,
    AuthorizationException,
    BadRequestError,
    NotFoundError,
)
from msfwk.desp.serco_logs.models import SercoLog
from msfwk.utils.logging import get_logger

from search.models.constants import CLIENT_NOT_SETUP_MESSAGE
from search.models.elastic import MultiSearchQuery, SearchQuery, SortQuery
from search.models.exceptions import ConnectivityError, SearchIndexError
from search.models.responses import (
    SearchableDocument,
    SearchCountResponse,
    SearchResponse,
)

logger = get_logger("application")

client: AsyncElasticsearch | None = None
LOG_INDEX = "serco-logs"

def get_url_from_config(elastic_config: dict) -> str:
    """Get the url from the elastic config"""
    protocol = "https" if elastic_config.get("ssl", True) else "http"
    server = elastic_config.get("server", "localhost")
    port = elastic_config.get("port", "9200")
    return f"{protocol}://{server}:{port}/"

async def init_connectivity(config: dict) -> AsyncElasticsearch:
    """Create an elasticsearch connection based on the configuration"""
    logger.info("Connecting to elasticsearch....")
    global client  # noqa: PLW0603 TODO is it needed
    elastic_config = config.get("elastic", {})
    connection_url = get_url_from_config(elastic_config)
    logger.debug("Connecting to  %s", connection_url)
    try:
        client = AsyncElasticsearch(
            connection_url, verify_certs=elastic_config.get("ssl_check", True), request_timeout=10
        )
        logger.info("Connection info %s", (await client.info()).get("version").get("number"))
    except ConnectionError as E:
        message = f"Failed to connect to elasticsearch {connection_url}"
        logger.exception(message)
        raise ConnectivityError(message) from E
    except AuthenticationException as E:
        message = f"Failed to authenticate to elasticsearch {connection_url}"
        logger.exception(message)
        raise ConnectivityError(message) from E
    else:
        return client


async def check_index_exists(index_name: str) -> bool:
    """Check that an index exists in elasticsearch

    Args:
        index_name (str): the name of the index

    Raises:
        SearchIndexError: if the client is not ready or the index search causes an error

    Returns:
        bool: true if it exists
    """
    logger.debug("Checking index %s", index_name)
    if client is None:
        message = CLIENT_NOT_SETUP_MESSAGE
        logger.error(message)
        raise SearchIndexError(message)
    try:
        response: HeadApiResponse = await client.indices.exists(index=index_name)
    except Exception as e:
        message = "Error when lookup the index"
        logger.exception(message)
        raise SearchIndexError(message, e) from e
    return response.body


def _get_index(asset_type: AssetType) -> str:
    return f"{asset_type.name}_index"


async def init(config: dict) -> bool:
    """Initialise the indexes in the search engine.
    We are using an index per asset type to ease the search
    """
    logger.info("Search Service initiating....")
    global client  # noqa: PLW0603
    client = await init_connectivity(config)
    if client is None:
        # If we failed to connect we indicate to the framework that we are not ready
        return False

    for asset_type in AssetType:
        index = _get_index(asset_type)
        if not await check_index_exists(index):
            mappings = {
                "properties": {
                    "name": {"type": "text"},
                    "created_at": {"type": "date"},
                    "categoryId": {"type": "keyword"},
                    "source": {"type": "keyword"},
                    "metadata": {"type": "nested"},
                    "metadata_json": {"type": "text"},
                    "likes_count": {"type": "integer"},
                    "downloads_count": {"type": "integer"},
                    "views_count": {"type": "integer"},
                    "geo_shapes": {
                        "type": "nested",
                        "properties": {
                            "file_path": {"type": "keyword"},
                            "shape": {"type": "geo_shape"},
                            "crs": {"type": "keyword"},
                        },
                    },
                }
            }
            await client.indices.create(index=index, mappings=mappings)

    # Init log index
    if not await check_index_exists(LOG_INDEX):
        log_mapping = {
            "properties": {
                "@timestamp": {"type": "date"},
                "event_type": {"type": "keyword"},
                "service_name": {"type": "keyword"},
                "transaction_id": {"type": "keyword"},
            }
        }
        await client.indices.create(index=LOG_INDEX, mappings=log_mapping)

    return True


async def destroy() -> None:
    """Shutodown properly the service"""
    if client is None:
        return
    logger.info("Search Service shutting down....")
    await client.close()


def _prettify_response(hit: dict) -> SearchableDocument:
    """Change the response to be a readble dictionary"""
    logger.debug("Hit: %s", hit)
    return SearchableDocument.from_hit(hit)


async def _get_count_in_index(index: str, query: Mapping[str, Any] | None) -> int:
    """Get count on a single index

    Raises
        ConnectivityError: __desc__
    """
    if client is None:
        message = CLIENT_NOT_SETUP_MESSAGE
        logger.error(message)
        raise ConnectivityError(message)
    try:
        logger.debug("Counting with %s, %s", index, query)
        response = await client.count(index=index, query=query)
        logger.debug("Raw count result %s", response)
        return response["count"]
    except AuthorizationException as ae:
        message = "Failed to authenticate to elasticsearch"
        logger.exception(message, exc_info=ae)
        raise ConnectivityError(message) from ae
    except Exception as E:
        logger.exception("Exception while counting on elastic : %s", E)
        raise E


async def _search_in_index(
    index: str, query: Mapping[str, Any] | None, offset: int, limit: int, sort: list[SortQuery] | None = None
) -> list[SearchableDocument]:
    """Search on a single index

    Raises
        ConnectivityError: __desc__
    """
    if client is None:
        message = CLIENT_NOT_SETUP_MESSAGE
        logger.error(message)
        raise ConnectivityError(message)

    sort_queries = []
    if sort is not None:
        # adds the _score to the sort queries if not exist
        sort.insert(0, SortQuery(field="_score", order="desc"))
        sort_queries = list({sort_query.field: sort_query.to_es_format() for sort_query in sort}.values())
    try:
        logger.debug("Searching with %s, %s, %s, %s, %s", index, query, sort_queries, offset, limit)
        response = await client.search(index=index, query=query, sort=sort_queries, from_=offset, size=limit)
        logger.debug("Raw search result %s", response)
        # If sorting is applied, return hits in the same order as response
        if sort is not None:
            filtered_response = response["hits"]["hits"]
        else:
            filtered_response = [hit for hit in response["hits"]["hits"] if hit.get("_score", 0.0) > 0.0]
        if len(filtered_response) == 0:
            logger.debug("Search returned no results.")
            return []
        return [_prettify_response(hit) for hit in filtered_response]
    except AuthorizationException as ae:
        message = "Failed to authenticate to elasticsearch"
        logger.exception(message, exc_info=ae)
        raise ConnectivityError(message) from ae


def _is_return_all(query: SearchQuery) -> bool:
    """Check if the search criteria is filled"""
    return query.text == "" and query.documentCategory is None and query.documentSource is None


def _build_query(query: SearchQuery) -> Mapping[str, Any] | None:
    """Builds the elastic query from the SearchQuery Object.
    If the query contains metadata information, it is considered advanced, otherwise it is simple.
    Simple query : The query is a logical OR on the name + metadata (with fuzzy and wildcard)
    Advanced query : The query is a logical OR on the name (fuzzy and wildcard) + the key/values in the metadata
    """
    logger.debug("Query: '%s'", query)
    if _is_return_all(query):
        return {"match_all": {}}

    ## Common query base :
    search_query = {
        "bool": {
            "should": [],
            "filter": [],
            "must": [],
        }
    }

    ## Exact name query :
    if query.name is not None:
        search_query["bool"]["must"].append({"match": {"name": query.name}})

    ## Base name query :
    if query.text != "":
        search_query["bool"]["should"].append({"wildcard": {"name": {"value": f"*{query.text.lower()}*"}}})
        search_query["bool"]["should"].append({"fuzzy": {"name": {"value": query.text.lower(), "fuzziness": 1}}})

    ## Document Source
    if query.documentSource is not None:
        search_query["bool"]["filter"].append({"term": {"source": query.documentSource.value}})

    ## Categories
    if query.documentCategory is not None and len(query.documentCategory) > 0:
        search_query["bool"]["filter"].append({"terms": {"categoryId": query.documentCategory}})

    ## Simple queries add 'brute force' matching on metadata
    if query.metadatas is None:
        logger.debug("Building a simple query, adding fuzzy and wildcard metadata...")
        search_query["bool"]["should"].append(
            {"fuzzy": {"metadata_json": {"value": query.text.lower(), "fuzziness": 1}}}
        )
        search_query["bool"]["should"].append({"wildcard": {"metadata_json": {"value": f"*{query.text.lower()}*"}}})

    ## Metadata filters only for advanced mode
    else:
        logger.debug("Building an advanced query, adding metadata matching...")
        search_query["bool"]["must"].append(
            {
                "nested": {
                    "path": "metadata",
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match": {
                                        f"metadata.{key}": f"{str(query.metadatas[key]).lower()}",
                                    }
                                }
                                for key in query.metadatas.keys()
                            ]
                        }
                    },
                }
            }
        )

    if query.geo_bounding_box is not None:
        logger.debug("Adding geo_bounding_box query...")
        search_query["bool"]["filter"].append(
            {
                "nested": {
                    "path": "geo_shapes",
                    "query": {
                        "geo_shape": {
                            "geo_shapes.shape": {
                                "shape": {
                                    "type": "envelope",
                                    "coordinates": [
                                        [
                                            query.geo_bounding_box.top_left["lat"],
                                            query.geo_bounding_box.top_left["lon"],
                                        ],
                                        [
                                            query.geo_bounding_box.bottom_right["lat"],
                                            query.geo_bounding_box.bottom_right["lon"],
                                        ],
                                    ],
                                },
                                "relation": "intersects",
                            }
                        }
                    },
                }
            },
        )

    return search_query


async def search(
    query: SearchQuery, offset: int, limit: int, sort_by: list[SortQuery] | None, count: bool
) -> SearchResponse | SearchCountResponse:
    """Do the search query on a specific index or all the indexes"""
    logger.info("Searching with : %s, (%s,%s), %s", query.model_dump(), offset, limit, sort_by)
    assets = []
    matching_query = _build_query(query)
    logger.debug("matching_query %s", matching_query)

    # Query on all assets (no asset type specified)
    if query.documentType is None:
        for asset_type in AssetType:
            partial_results = await _search_in_index(_get_index(asset_type), matching_query, offset, limit, sort_by)
            assets.extend(partial_results)

    # Query for a single asset type
    else:
        assets = await _search_in_index(_get_index(query.documentType), matching_query, offset, limit, sort_by)

    if count:
        return SearchCountResponse(count=await _get_count_for_query(query), assets=assets)
    return SearchResponse(assets=assets)


async def _get_count_for_query(query: SearchQuery) -> int:
    """Returns the total hits from a query"""
    matching_query = _build_query(query)
    if query.documentType is None:
        total_hits = 0
        for asset_type in AssetType:
            total_hits += await _get_count_in_index(_get_index(asset_type), matching_query)
        return total_hits
    return await _get_count_in_index(_get_index(query.documentType), matching_query)


async def register(document: SearchableDocument) -> None:
    """Register a document in the index

    Args:
        document (SearchableDocument): a document that will be added to the index

    Raises:
        SearchIndexError: when the index built from the documentType is not found in ES
        ConnectivityError: when the client is not ready to connect
    """
    if client is None:
        message = CLIENT_NOT_SETUP_MESSAGE
        logger.error(message)
        raise ConnectivityError(message)
    index = _get_index(document.documentType)
    if not await check_index_exists(index):
        message = f"{index} index was not setup earlier"
        logger.error(message)
        raise SearchIndexError(message)
    try:
        logger.debug("Registrating the document %s with data %s", document.id, document.to_dict())
        await client.index(index=index, id=document.id, document=document.to_dict())
    except BadRequestError as bre:
        logger.exception("Failed to register document", exc_info=bre)
        raise bre


async def delete(document_type: AssetType, document_id: str) -> bool:
    """Remove a document from the search engine

    Args:
        document_id (str): The id of the document
        document_type (AssetType): The asset type
    Returns:
        bool: if it is successful
    """
    if client is None:
        message = CLIENT_NOT_SETUP_MESSAGE
        logger.error(message)
        raise ConnectivityError(message)
    index = _get_index(document_type)
    if not await check_index_exists(index):
        message = f"{index} index was not setup earlier"
        logger.error(message)
        raise SearchIndexError(message)
    try:
        logger.debug("Deleting the document %s", document_id)
        await client.delete(index=index, id=document_id)
    except BadRequestError as bre:
        logger.exception("Failed to delete document", exc_info=bre)
    except NotFoundError as nfe:
        logger.exception("Failed to delete document", exc_info=nfe)
    return False


async def clear_index_from_elastic(document_type: AssetType) -> dict:
    """Clear the cache of a specific index in Elasticsearch.

    Args:
        document_type (AssetType): The type of document whose index should be cleared.

    Returns:
        dict: Information about the cleared cache.

    Raises:
        ConnectivityError: If the client is not properly setup.
        SearchIndexError: If the index does not exist.
    """
    if client is None:
        message = CLIENT_NOT_SETUP_MESSAGE
        logger.error(message)
        raise ConnectivityError(message)
    index = _get_index(document_type)
    if not await check_index_exists(index):
        message = f"{index} index was not setup earlier"
        logger.error(message)
        raise SearchIndexError(message)
    await client.indices.flush()
    info = await client.indices.clear_cache(index=index)
    logger.info(info)
    return info


async def delete_index_from_elastic(document_type: AssetType) -> dict:
    """Delete a specific index from Elasticsearch.

    Args:
        document_type (AssetType): The type of document whose index should be deleted.

    """
    if client is None:
        message = CLIENT_NOT_SETUP_MESSAGE
        logger.error(message)
        raise ConnectivityError(message)
    index = _get_index(document_type)
    if not await check_index_exists(index):
        message = f"{index} index was not setup earlier"
        logger.error(message)
        raise SearchIndexError(message)
    info = await client.indices.delete(index=index)
    logger.info(info)
    return info


async def fetch_index_from_elastic(document_type: AssetType) -> None:
    """Fetch the mappings and stats of a specific index from Elasticsearch.

    Args:
        document_type (AssetType): The type of document whose index should be fetched.
    """
    if client is None:
        message = CLIENT_NOT_SETUP_MESSAGE
        logger.error(message)
        raise ConnectivityError(message)
    index = _get_index(document_type)
    if not await check_index_exists(index):
        message = f"{index} index was not setup earlier"
        logger.error(message)
        raise SearchIndexError(message)
    logger.info("mappings : %s", await client.indices.get_mapping(index=index))
    logger.info("stats : %s", await client.indices.stats(index=index))


async def multi_search(multi_query: MultiSearchQuery, limit: int) -> dict[str, SearchCountResponse]:
    """Perform multiple searches with different sort criteria using Elasticsearch's _msearch endpoint

    Args:
        multi_query (MultiSearchQuery): The multi-query containing multiple search queries with their sort criteria
        limit (int): The maximum number of results to return for each query

    Returns:
        dict[str, SearchResponse]: A dictionary mapping sort criteria to search results
    """
    if client is None:
        message = CLIENT_NOT_SETUP_MESSAGE
        logger.error(message)
        raise ConnectivityError(message)

    results = {key: None for key in multi_query.queries}

    try:
        for key, (query, sort_by) in multi_query.queries.items():
            response = await search(query, 0, limit, sort_by, count=True)
            results[key] = response
        logger.debug("results: %s", results)

    except Exception as e:
        message = "Failed to perform multi-search"
        logger.exception(message, exc_info=e)
        raise ConnectivityError(message) from e

    return results


async def push_log_to_elasticsearch(log: SercoLog, index_name: str = LOG_INDEX) -> bool:
    """Pushes a SercoLog to Elasticsearch asynchronously."""
    try:
        doc = log.to_log()
        await client.index(index=index_name, document=doc)
    except Exception as ee:
        message = f"Error sending log to Elasticsearch: {ee}"
        logger.exception(message, exc_info=ee)
        return False
    return True


async def get_all_logs(offset: int = 0, limit: int = 1000) -> list:
    """Retrieve all logs from the serco-logs index."""
    if client is None:
        message = CLIENT_NOT_SETUP_MESSAGE
        logger.error(message)
        raise ConnectivityError(message)

    try:
        # Prepare the query to fetch all logs
        query = {"match_all": {}}

        # Perform the search on the log index
        response = await client.search(
            index=LOG_INDEX,  # The index containing logs
            query=query,
            from_=offset,  # Offset for pagination
            size=limit,  # Limit for pagination
        )

        # Extract hits (log entries)
        logs = response.get("hits", {}).get("hits", [])

        # Log the number of logs retrieved
        logger.info("Retrieved %s logs from the %s index.", len(logs), LOG_INDEX)

        # Process the hits to return readable format
        return [hit["_source"] for hit in logs]

    except NotFoundError as e:
        message = f"Index {LOG_INDEX} not found: {e}"
        logger.exception(message)
        raise SearchIndexError(message) from e
    except Exception as e:
        logger.exception("Error occurred while retrieving logs.")
        raise ConnectivityError("Error occurred while retrieving logs.", e) from e
