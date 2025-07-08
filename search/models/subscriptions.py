"""Models for the subscriptions API"""

from datetime import datetime
from uuid import UUID

from msfwk.models import BaseModelAdjusted

from search.models.elastic import SearchQuery


class Subscription(BaseModelAdjusted):
    """A subscription to a query"""

    id: UUID
    name: str | None = None
    query: SearchQuery
    despUserId: str
    created_at: datetime


class SubscriptionCreate(BaseModelAdjusted):
    """A subscription to a query"""

    name: str | None = None
    query: SearchQuery


class SubscriptionUpdate(BaseModelAdjusted):
    """A subscription to a query"""

    name: str | None = None
    query: SearchQuery | None = None
