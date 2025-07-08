"""Models for the search API"""

from msfwk.utils.logging import get_logger
from pydantic import BaseModel

from search.models.elastic import SearchableDocument

logger = get_logger(__name__)


class SearchResponse(BaseModel):
    """Hold the result of a search"""

    assets: list[SearchableDocument] = []


class SearchCountResponse(SearchResponse):
    """Hold the result of a search"""

    count: int


class RegisterResponse(BaseModel):
    """hold the register response"""

    status: str


class MultiSearchResponse(BaseModel):
    """Hold the result of a multi-search"""

    results: dict[str, SearchCountResponse]
