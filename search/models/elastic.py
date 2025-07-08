import json
from datetime import datetime
from typing import Any

from despsharedlibrary.schemas.collaborative_schema import AssetType, SourceType
from msfwk.models import BaseModelAdjusted
from msfwk.utils.logging import get_logger
from pydantic import BaseModel

logger = get_logger(__name__)
ISO_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"  # +00:00 comes from UTC


class GeoShape(BaseModel):
    """Represents a geographic shape with its file path, GeoJSON shape data, and coordinate reference system."""

    file_path: str
    shape: dict  # Assuming GeoJSON-like structure; adjust if needed
    crs: str


class SearchableDocument(BaseModelAdjusted):
    """Class describing a document that will be registered to the index"""

    # The id of the asset in the relational database
    id: str
    # The type that will drive the index to use
    documentType: AssetType  # noqa: N815
    # The name of the asset
    name: str
    # Meta data about the asset to be indexed
    metadata: dict[str, str]
    date: datetime
    # Search score the closer it gets to 1, the closer is from the search
    score: float = 0.0
    # Asset category
    categoryId: str  # noqa: N815
    # source of the asset
    source: SourceType = SourceType.user
    # nb of likes
    likes_count: int

    downloads_count: int = -1
    views_count: int = -1
    geo_shapes: list[GeoShape] = []

    def to_dict(self) -> dict[str, Any]:
        """Make the class serializable"""
        return {
            "id": self.id,
            "name": self.name,
            "score": self.score,
            "metadata": self.metadata,
            "metadata_json": json.dumps(self.metadata),
            "date": self.date.isoformat(),
            "type": self.documentType.value,
            "categoryId": self.categoryId,
            "source": self.source.value,
            "likes_count": self.likes_count,
            "downloads_count": self.downloads_count,
            "geo_shapes": [gs.model_dump() for gs in self.geo_shapes],
        }

    @staticmethod
    def from_hit(hit: dict) -> "SearchableDocument":
        """Build the SearchableDocument from a hit (Elastic search returned object)"""
        searchable = SearchableDocument(
            id=hit["_source"]["id"],
            name=hit["_source"]["name"],
            metadata=hit["_source"]["metadata"],
            documentType=AssetType[hit["_source"]["type"]],
            date=datetime.fromisoformat(hit["_source"]["date"]),
            categoryId=hit["_source"].get("categoryId", "0"),
            source=SourceType[hit["_source"].get("source", SourceType.user.value)],
            likes_count=hit["_source"].get("likes_count", -1),
            downloads_count=hit["_source"].get("downloads_count", -1),
            geo_shapes=[GeoShape(**gs) for gs in hit["_source"].get("geo_shapes", [])],
        )
        searchable.score = hit["_score"]
        return searchable


class GeoBoundingBox(BaseModel):
    """Hold the geographic data for search criteria"""

    top_left: dict[str, float]
    bottom_right: dict[str, float]


class SearchQuery(BaseModel):
    """Hold the search criteria"""

    documentType: AssetType | None = None  # noqa: N815
    documentSource: SourceType | None = None  # noqa: N815
    documentCategory: list[str] | None = None  # noqa: N815
    metadatas: dict[str, int | str] | None = None
    text: str
    geo_bounding_box: GeoBoundingBox | None = None
    name: str | None = None  # exact match on name


class SortQuery(BaseModel):
    """hold the sort criteria"""

    field: str
    order: str

    @classmethod
    def from_str(cls, sort_str: str) -> "SortQuery":
        """Convert the sort to the es format"""
        field, order = sort_str.split(":")[0], sort_str.split(":")[1] if ":" in sort_str else sort_str, "desc"
        return cls(field=field, order=order)

    def to_es_format(self) -> dict[str, str]:
        """Convert the sort to the es format"""
        return {self.field: {"order": self.order}}


def sorts_to_query_list(sorts: str | None) -> list[SortQuery]:
    """Convert to sort list use by elasticsearch"""
    if not sorts:
        return []
    sorts_array = sorts.split(",")

    # we need to add the score to the sorts, so elasticsearch continues to use it
    if "_score" not in sorts_array:
        sorts_array.append("_score")
    es_sort_query = []
    for sort_param in sorts_array:
        if ":" not in sort_param:
            sort_query = SortQuery(field=sort_param, order="desc")
        else:
            sort_query = SortQuery.from_str(sort_param)
        es_sort_query.append(sort_query)
    logger.debug("es_sort_query: %s", es_sort_query)
    return es_sort_query


def parse_geo_bounding_box(geo: str) -> GeoBoundingBox | None:
    """Parse the geo bounding box from the input string."""
    if not geo:
        return None

    try:
        coordinates = json.loads(geo)
        if len(coordinates) == 2 and all(len(coord) == 2 for coord in coordinates):  # noqa: PLR2004
            logger.warning(coordinates)
            return GeoBoundingBox(
                top_left={"lat": coordinates[0][0], "lon": coordinates[0][1]},
                bottom_right={"lat": coordinates[1][0], "lon": coordinates[1][1]},
            )

    except json.JSONDecodeError:
        return None


class MultiSearchQuery(BaseModel):
    """Hold multiple search criteria with different sort orders"""

    queries: dict[str, tuple[SearchQuery, list[SortQuery]]]
