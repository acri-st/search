"""Declare the routes for the search API"""

import json

from despsharedlibrary.schemas.collaborative_schema import AssetType, SourceType
from fastapi import APIRouter, BackgroundTasks, Query
from msfwk import database
from msfwk.application import openapi_extra
from msfwk.desp.serco_logs.models import SercoLog
from msfwk.models import BaseDespResponse, DespResponse
from msfwk.utils.logging import get_logger

from search.models.constants import (
    FAILED_TO_REGISTER_ASSET,
    FAILED_TO_REGISTER_ASSET_DUE_TO_INDEX,
    FAILED_TO_SEARCH_ASSET,
)
from search.models.elastic import MultiSearchQuery, SearchQuery, parse_geo_bounding_box, sorts_to_query_list
from search.models.exceptions import SearchIndexError
from search.models.responses import (
    MultiSearchResponse,
    RegisterResponse,
    SearchableDocument,
    SearchCountResponse,
    SearchResponse,
)
from search.services.metadatas import get_metadatas_in_db
from search.services.search import (
    clear_index_from_elastic,
    delete,
    delete_index_from_elastic,
    fetch_index_from_elastic,
    get_all_logs,
    multi_search,
    push_log_to_elasticsearch,
    register,
    search,
)
from search.services.subscriptions import (
    check_subscription_and_send_mails,
    get_subscriptions_in_db,
)

FAILED_TO_PERFORM_REGISTER = "Failed to perform register"

logger = get_logger("application")

router = APIRouter()


@router.get(
    "/search",
    summary="Search in the engine based on criteria",
    response_model=BaseDespResponse[SearchResponse],
    response_description="The list of assets matching the criteria",
    tags=["search"],
    openapi_extra=openapi_extra(secured=False, roles=[]),
)
async def search_documents(
    q: str = "",
    type: AssetType | None = None,
    categories: str | None = None,
    source: SourceType | None = None,
    offset: int = 0,
    limit: int = 10,
    sorts: str | None = None,
    count: bool = False,
    metadata: str | None = Query(
        default=None, description="json-formatted string with key / values to filter on specific metadata fields"
    ),
    geo: str | None = Query(default=None, description="Geographic bounding box as [[lat,lng],[lat,lng]]"),
) -> DespResponse[SearchResponse | SearchCountResponse]:
    """Search document in the engine
    categories => comma separated
    """
    logger.info(
        "Searching assets... with q=%s, type=%s, source=%s, categories=%s, metadata=%s, geo=%s",
        q,
        type,
        source,
        categories,
        metadata,
        geo,
    )
    category_list = [] if categories is None else categories.split(",")

    # Load metadata from the json dump
    metadata_fields: dict[str, str | int] | None = json.loads(metadata) if metadata is not None else None

    # Parse geo bounding box
    geo_bounding_box = parse_geo_bounding_box(geo)
    if geo and geo_bounding_box is None:
        return DespResponse(
            data={},
            error="Invalid geo bounding box format",
            code="INVALID_GEO_FORMAT",
            http_status=400,
        )

    search_query = SearchQuery(
        documentType=type,
        text=q,
        documentCategory=category_list,
        documentSource=source,
        metadatas=metadata_fields,
        geo_bounding_box=geo_bounding_box,
    )
    search_response: SearchResponse | SearchCountResponse = SearchResponse()
    try:
        # TODO(odaumas): check that the category exists
        sort_by = sorts_to_query_list(sorts)
        search_response = await search(search_query, offset, limit, sort_by, count)
        logger.debug("Found assets: %s", search_response)
    except Exception as e:
        logger.exception("Failed to perform search for", exc_info=e)
        return DespResponse(
            data=search_response.model_dump(mode="json"),
            error=str(e),
            code="FAILED_TO_SEARCH_ASSET",
            http_status=500,
        )
    return DespResponse(data=search_response.model_dump(mode="json"))


@router.post(
    "/register",
    summary="Register an asset inside the engine",
    response_model=BaseDespResponse[RegisterResponse],
    response_description="A message indicating that everything went well or an error code",
    tags=["search"],
    openapi_extra=openapi_extra(secured=True, roles=["user"], internal=True),
)
async def register_document(
    doc: SearchableDocument, background_tasks: BackgroundTasks
) -> DespResponse[RegisterResponse]:
    """Method to register document in the Elasticsearch database

    Args:
        doc (SearchableDocument): the document to register
        background_tasks (BackgroundTasks): background tasks runner for async operations

    Returns:
        JSONReponse: 201 if everything went fine, 500 if something went wrong
    """
    logger.info("Registrating asset type %s...", doc.documentType.value)
    response: RegisterResponse = RegisterResponse(status="created")
    try:
        logger.info(
            "Registration : doc.geo_shapes missing, will check previous document to save its geoshape if it exists: %s",
            doc.geo_shapes,
        )
        if len(doc.geo_shapes) == 0:
            logger.info("Registration : no geoshape transmitted.")
            # No geoshape transmitted, we need to fetch the document first to save its geoshape if it exists
            previous_doc = await search(SearchQuery(name=doc.name, text=doc.name), 0, 1, None, count=False)
            logger.info("Registration : previous_doc: %s", previous_doc.model_dump())
            if len(previous_doc.assets) > 0:
                if len(previous_doc.assets[0].geo_shapes) > 0:
                    logger.info(
                        "Registration : Using previous geoshapes %s",
                        previous_doc.assets[0].geo_shapes,
                    )
                    doc.geo_shapes = previous_doc.assets[0].geo_shapes
                    logger.info("Registration : new doc with geoshape: %s", doc.model_dump())
                else:
                    logger.info("Registration : no previous geoshape found")
            else:
                logger.info("Registration : no previous document found")
        else:
            logger.info("Registration : overwriting full document: %s", doc.geo_shapes)

        await register(doc)

        logger.info("Asset registered successfully, now checking it against all subscriptions")
        async with database.get_schema("collaborative").get_async_session() as db_session:
            subscriptions = await get_subscriptions_in_db(db_session)
            current_metadatas = await get_metadatas_in_db(db_session)

            logger.info("Adding background task to check subscriptions")
            background_tasks.add_task(check_subscription_and_send_mails, doc, subscriptions, current_metadatas)

    except SearchIndexError as sie:
        message = "Failed to connect to the index"
        logger.exception(message, exc_info=sie)
        return DespResponse(data=response, error=message, code=FAILED_TO_REGISTER_ASSET_DUE_TO_INDEX, http_status=400)
    except Exception as e:
        logger.exception(FAILED_TO_PERFORM_REGISTER, exc_info=e)
        return DespResponse(data=response, error=str(e), code=FAILED_TO_REGISTER_ASSET, http_status=500)

    return DespResponse(data=response)


@router.post(
    "/index/{document_type}/clear",
    summary="Clear an index cache inside the engine",
    response_model=BaseDespResponse[RegisterResponse],
    response_description="A message indicating that everything went well or an error code",
    tags=["search"],
    openapi_extra=openapi_extra(secured=True, roles=["admin"], internal=True),
)
async def clear_index_cache(document_type: AssetType) -> DespResponse[RegisterResponse]:
    """Method to delete an index in the Elasticsearch database

    Args:
        document_type (AssetType): The type of document
    """
    logger.info("Clearing index %s...", document_type)
    try:
        await clear_index_from_elastic(document_type)
        return DespResponse(data={"status": "cache cleared"})
    except Exception as e:
        logger.exception(FAILED_TO_PERFORM_REGISTER, exc_info=e)
        return DespResponse(
            data={"status": "failed"}, error=str(e), code=FAILED_TO_REGISTER_ASSET_DUE_TO_INDEX, http_status=500
        )


@router.delete(
    "/index/{document_type}",
    summary="Clear an index cache inside the engine",
    response_model=BaseDespResponse[RegisterResponse],
    response_description="A message indicating that everything went well or an error code",
    tags=["search"],
    openapi_extra=openapi_extra(secured=True, roles=["admin"], internal=True),
)
async def delete_index(document_type: AssetType) -> DespResponse[RegisterResponse]:
    """Method to delete an index in the Elasticsearch database

    Args:
        document_type (AssetType): The type of document
    """
    logger.info("Deleting index %s...", document_type)
    try:
        await delete_index_from_elastic(document_type)
        return DespResponse(data={"status": "index deleted"})
    except Exception as e:
        logger.exception("Failed to perform index delete", exc_info=e)
        return DespResponse(
            data={"status": "could not delete index"},
            error=str(e),
            code=FAILED_TO_REGISTER_ASSET_DUE_TO_INDEX,
            http_status=500,
        )


@router.get(
    "/index/{document_type}/analysis",
    summary="Get info on an index inside the engine",
    response_model=BaseDespResponse[RegisterResponse],
    response_description="A message indicating that everything went well or an error code",
    tags=["search"],
    openapi_extra=openapi_extra(secured=True, roles=["admin"], internal=True),
)
async def get_index_info(document_type: AssetType) -> DespResponse[RegisterResponse]:
    """Method to delete an index in the Elasticsearch database

    Args:
        document_type (AssetType): The type of document
    """
    logger.info("Fetching index %s...", document_type)
    try:
        await fetch_index_from_elastic(document_type)
        return DespResponse()
    except Exception as e:
        logger.exception(FAILED_TO_PERFORM_REGISTER, exc_info=e)
        return DespResponse(data={}, error=str(e), code=FAILED_TO_REGISTER_ASSET_DUE_TO_INDEX, http_status=500)


@router.delete(
    "/documents/{document_type}/{document_id}",
    summary="Delete an asset inside the engine",
    response_model=BaseDespResponse[RegisterResponse],
    response_description="A message indicating that everything went well or an error code",
    tags=["search"],
    openapi_extra=openapi_extra(secured=True, roles=["user"], internal=True),
)
async def delete_document(document_type: AssetType, document_id: str) -> DespResponse[RegisterResponse]:
    """Method to delete document in the Elasticsearch database

    Args:
        document_id (str): the document to delete
        document_type (AssetType): The type of document

    Returns:
        JSONReponse: 201 if everything went fine, 500 if something went wrong
    """
    logger.info("Deleting asset %s...", document_id)
    response: RegisterResponse = {}
    try:
        await delete(document_type, document_id)
        response = {"status": "deleted"}
    except SearchIndexError as sie:
        message = "Failed to connect to the index"
        logger.exception(message, exc_info=sie)
        return DespResponse(data=response, error=message, code=FAILED_TO_REGISTER_ASSET_DUE_TO_INDEX, http_status=400)
    except Exception as e:
        logger.exception(FAILED_TO_PERFORM_REGISTER, exc_info=e)
        return DespResponse(data=response, error=str(e), code=FAILED_TO_REGISTER_ASSET, http_status=500)
    return DespResponse(response)


@router.post(
    "/multi-search",
    summary="Perform multiple searches with different sort criteria",
    response_model=BaseDespResponse[dict[str, SearchResponse]],
    response_description="A dictionary mapping sort criteria to search results",
    tags=["search"],
    openapi_extra=openapi_extra(secured=True, roles=[], internal=True),
)
async def multi_search_documents(
    multi_query: MultiSearchQuery,
    limit: int = 20,
) -> DespResponse[MultiSearchResponse]:
    """Perform multiple searches with different sort criteria

    Example:
        POST /multi-search
        {
            "queries": [
                [
                    {
                        "text": "",
                        "documentType": "dataset"
                    },
                    [{"likes_count": {"order": "desc"}}]
                ],
                [
                    {
                        "text": "",
                        "documentType": "dataset"
                    },
                    [{"downloads_count": {"order": "desc"}}]
                ]
            ]
        }
    """
    try:
        results = await multi_search(multi_query, limit)
        return DespResponse(data=MultiSearchResponse(results=results))
    except Exception as e:
        logger.exception("Failed to perform multi-search", exc_info=e)
        return DespResponse(
            data={},
            error=str(e),
            code=FAILED_TO_SEARCH_ASSET,
            http_status=500,
        )


@router.post(
    "/logs",
    summary="add a single log",
    response_model=BaseDespResponse[dict],
    response_description="A dictionary mapping sort criteria to search results",
    tags=["search"],
    openapi_extra=openapi_extra(secured=True, roles=[], internal=True),
)
async def post_log(log: SercoLog) -> DespResponse[dict]:
    """API endpoint to push a log entry to Elasticsearch."""
    logger.info("Posting log: %s", log.to_log())

    success = await push_log_to_elasticsearch(log)
    if not success:
        return DespResponse(data={"status": "Failed"}, http_status=500)
    return DespResponse(data={"status": "Success"})


@router.get(
    "/logs",
    summary="get all log",
    response_model=BaseDespResponse[dict],
    response_description="A dictionary mapping sort criteria to search results",
    tags=["search"],
    openapi_extra=openapi_extra(secured=False, roles=[], internal=False),
)
async def get_log() -> DespResponse[dict]:
    """API endpoint to push a log entry to Elasticsearch."""
    logger.info("Getting log")
    logs = await get_all_logs()
    return DespResponse(data={"logs": logs})
