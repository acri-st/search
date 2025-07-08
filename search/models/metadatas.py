"""Models for the subscriptions API"""

from uuid import UUID

from despsharedlibrary.schemas.collaborative_schema import AssetType, MetadataType
from msfwk.models import BaseModelAdjusted


class Metadata(BaseModelAdjusted):
    """Metadata information"""

    id: UUID
    name: str
    label: str
    description: str = ""
    type: MetadataType
    asset_type: AssetType | None  # None if it works for all asset types
    options: dict | None = {}
    section: str | None = None
    required: bool = False
    queryable: bool = False
    validator: str | None = None
    priority: int | None = None


class MetadataCreate(BaseModelAdjusted):
    """Metadata information to create"""

    name: str
    label: str
    description: str = ""
    type: MetadataType
    asset_type: AssetType | None = None  # None if it works for all asset types
    options: dict = {}
    section: str | None = None
    required: bool = False
    queryable: bool = False
    validator: str | None = None
    priority: int | None = None


class MetadataUpdate(BaseModelAdjusted):
    """Metadata information to update"""

    name: str | None = None
    label: str | None = None
    description: str | None = None
    type: MetadataType | None = None
    asset_type: AssetType | None = None
    options: dict | None = None
    section: str | None = None
    required: bool | None = None
    queryable: bool | None = None
    validator: str | None = None
    priority: int | None = None
