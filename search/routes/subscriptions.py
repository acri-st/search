"""Declare the routes for the subscriptions API.
Users can subscribe to a query, and be notified when a new matching asset is created.
"""

from uuid import UUID

from fastapi import APIRouter
from msfwk import database
from msfwk.application import openapi_extra
from msfwk.exceptions import DespGenericError
from msfwk.models import BaseDespResponse, DespResponse
from msfwk.utils.logging import get_logger
from msfwk.utils.user import get_current_user

from search.models.constants import USER_NOT_LOGGED, USER_NOT_LOGGED_MESSAGE
from search.models.subscriptions import Subscription, SubscriptionCreate, SubscriptionUpdate
from search.services.subscriptions import (
    create_subscription_in_db,
    delete_subscription_in_db,
    get_subscriptions_in_db,
    update_subscription_in_db,
)

logger = get_logger("application")
router = APIRouter()


@router.post(
    "/subscriptions",
    summary="Create a new subscription",
    tags=["subscriptions"],
    response_model=BaseDespResponse[Subscription],
    openapi_extra=openapi_extra(secured=True, roles=["user"]),
)
async def create_subscription(subscription: SubscriptionCreate) -> DespResponse[Subscription]:
    """Create a new subscription"""
    current_user = get_current_user()
    if current_user is None:
        raise DespGenericError(status_code=401, code=USER_NOT_LOGGED, message=USER_NOT_LOGGED_MESSAGE)

    async with database.get_schema("collaborative").get_async_session() as db_session:
        result = await create_subscription_in_db(db_session, subscription, despUserId=current_user.id)
        return DespResponse(data=result)


@router.get(
    "/subscriptions",
    summary="Get all subscriptions",
    tags=["subscriptions"],
    response_model=BaseDespResponse[list[Subscription]],
    openapi_extra=openapi_extra(secured=True, roles=["user"]),
)
async def get_all_subscriptions() -> DespResponse[list[Subscription]]:
    """Get all subscriptions"""
    current_user = get_current_user()
    if current_user is None:
        raise DespGenericError(status_code=401, code=USER_NOT_LOGGED, message=USER_NOT_LOGGED_MESSAGE)

    async with database.get_schema("collaborative").get_async_session() as db_session:
        result = await get_subscriptions_in_db(db_session, despUserId=current_user.id)
        return DespResponse(data=[subscription.model_dump() for subscription in result])


@router.get(
    "/subscriptions/{subscription_id}",
    summary="Get a subscription by ID",
    tags=["subscriptions"],
    response_model=BaseDespResponse[Subscription],
    openapi_extra=openapi_extra(secured=True, roles=["user"]),
)
async def get_subscription(subscription_id: UUID) -> DespResponse[Subscription]:
    """Get a subscription by ID"""
    current_user = get_current_user()
    if current_user is None:
        raise DespGenericError(status_code=401, code=USER_NOT_LOGGED, message=USER_NOT_LOGGED_MESSAGE)

    async with database.get_schema("collaborative").get_async_session() as db_session:
        result = await get_subscriptions_in_db(db_session, subscription_id=subscription_id, despUserId=current_user.id)
        return DespResponse(data=result[0].model_dump())


@router.patch(
    "/subscriptions/{subscription_id}",
    summary="Update a subscription and return the updated subscription",
    tags=["subscriptions"],
    response_model=BaseDespResponse[Subscription],
    openapi_extra=openapi_extra(secured=True, roles=["user"]),
)
async def update_subscription(subscription_id: UUID, update_data: SubscriptionUpdate) -> DespResponse[Subscription]:
    """Update a subscription"""
    current_user = get_current_user()
    if current_user is None:
        raise DespGenericError(status_code=401, code=USER_NOT_LOGGED, message=USER_NOT_LOGGED_MESSAGE)

    async with database.get_schema("collaborative").get_async_session() as db_session:
        result = await update_subscription_in_db(db_session, subscription_id, update_data, current_user.id)
        return DespResponse(data=result.model_dump())


@router.delete(
    "/subscriptions/{subscription_id}",
    summary="Delete a subscription",
    tags=["subscriptions"],
    response_model=BaseDespResponse[None],
    openapi_extra=openapi_extra(secured=True, roles=["user"]),
)
async def delete_subscription(subscription_id: UUID) -> DespResponse[None]:
    """Delete a subscription"""
    current_user = get_current_user()
    if current_user is None:
        raise DespGenericError(status_code=401, code=USER_NOT_LOGGED, message=USER_NOT_LOGGED_MESSAGE)

    async with database.get_schema("collaborative").get_async_session() as db_session:
        async with db_session.begin():
            await delete_subscription_in_db(db_session, subscription_id, current_user.id)
        return DespResponse(data=None)
