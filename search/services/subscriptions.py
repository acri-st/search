"""Service functions to manage subscriptions in the database"""

import json
import uuid
from datetime import datetime
from uuid import UUID

from despsharedlibrary.schemas.collaborative_schema import MetadataType
from msfwk import database
from msfwk.exceptions import DespGenericError
from msfwk.notification import NotificationTemplate, send_email_to_mq
from msfwk.utils.logging import get_logger
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from search.models.constants import (
    SUBSCRIPTION_CHECK_FAILED,
    SUBSCRIPTION_CREATE_FAILED,
    SUBSCRIPTION_DELETE_FAILED,
    SUBSCRIPTION_FETCH_FAILED,
    SUBSCRIPTION_NOT_FOUND_ERROR,
    SUBSCRIPTION_NOT_FOUND_MESSAGE,
    SUBSCRIPTION_UPDATE_FAILED,
)
from search.models.elastic import SearchableDocument
from search.models.mails import SubscriptionMatchMail
from search.models.metadatas import Metadata
from search.models.subscriptions import Subscription, SubscriptionCreate, SubscriptionUpdate
from search.services.auth_service import get_mail_from_desp_user_id

logger = get_logger("search")


async def create_subscription_in_db(
    session: AsyncSession, subscription: SubscriptionCreate, despUserId: str
) -> Subscription:
    """Create a new subscription in the database"""
    try:
        logger.debug("Creating subscription %s", subscription)
        subscription_table = database.get_schema("collaborative").tables["SearchSubscriptions"]
        now = datetime.now()  # noqa: DTZ005
        subscription_id = uuid.uuid4()

        statement = subscription_table.insert().values(
            {
                "id": subscription_id,
                "name": subscription.name,
                "query": json.loads(
                    subscription.query.model_dump_json()
                ),  # convert through json to avoid pydantic serialisation issues
                "despUserId": despUserId,
                "created_at": now,
            }
        )
        logger.info(statement)
        await session.execute(statement)
        await session.commit()

        return (await get_subscriptions_in_db(session, subscription_id=subscription_id))[0]

    except SQLAlchemyError as sae:
        message = "Failed to query the database to create the subscription"
        logger.exception(message, exc_info=sae)
        raise DespGenericError(status_code=500, code=SUBSCRIPTION_CREATE_FAILED, message=message) from sae


async def get_subscriptions_in_db(
    session: AsyncSession, subscription_id: UUID | None = None, despUserId: str | None = None
) -> list[Subscription]:
    """Get all subscriptions"""
    try:
        logger.debug("Getting subscriptions with subscription_id=%s and despUserId=%s", subscription_id, despUserId)
        subscription_table = database.get_schema("collaborative").tables["SearchSubscriptions"]
        statement = select(subscription_table)
        if subscription_id is not None:
            # Filter by subscription id
            statement = statement.where(subscription_table.c.id == subscription_id)
        if despUserId is not None:
            # Filter by despUserId
            statement = statement.where(subscription_table.c.despUserId == despUserId)

        result = await session.execute(statement)
        rows = result.fetchall()
        if len(rows) == 0 and subscription_id is not None:
            # specific subscription not found
            raise DespGenericError(
                status_code=404, code=SUBSCRIPTION_NOT_FOUND_ERROR, message=SUBSCRIPTION_NOT_FOUND_MESSAGE
            )
        # otherwise, return all subscriptions matching the filter
        return [Subscription(**row._mapping) for row in rows]
    except SQLAlchemyError as sae:
        message = "Failed to query the database to fetch the subscriptions"
        logger.exception(message, exc_info=sae)
        raise DespGenericError(status_code=500, code=SUBSCRIPTION_FETCH_FAILED, message=message) from sae


async def update_subscription_in_db(
    session: AsyncSession, subscription_id: UUID, update_data: SubscriptionUpdate, despUserId: str
) -> Subscription:
    """Update a subscription in the database"""
    try:
        logger.debug("Updating subscription %s with %s", subscription_id, update_data)
        subscription_table = database.get_schema("collaborative").tables["SearchSubscriptions"]
        statement = (
            subscription_table.update()
            .where(subscription_table.c.id == subscription_id)
            .where(subscription_table.c.despUserId == despUserId)
            .values(update_data.model_dump())
        )
        logger.info(statement)
        await session.execute(statement)
        await session.commit()
        return (await get_subscriptions_in_db(session, subscription_id=subscription_id))[0]
    except SQLAlchemyError as sae:
        message = "Failed to query the database to update the subscription"
        logger.exception(message, exc_info=sae)
        raise DespGenericError(status_code=500, code=SUBSCRIPTION_UPDATE_FAILED, message=message) from sae


async def delete_subscription_in_db(session: AsyncSession, subscription_id: UUID, despUserId: str) -> None:
    """Delete a subscription in the database"""
    try:
        logger.debug("Deleting subscription %s", subscription_id)
        subscription_table = database.get_schema("collaborative").tables["SearchSubscriptions"]
        # Check if subscription exists and get it in a single query
        statement = (
            select(subscription_table)
            .where(subscription_table.c.id == subscription_id)
            .where(subscription_table.c.despUserId == despUserId)
        )
        result = await session.execute(statement)
        if not result.fetchone():
            raise DespGenericError(
                status_code=404, code=SUBSCRIPTION_NOT_FOUND_ERROR, message=SUBSCRIPTION_NOT_FOUND_MESSAGE
            )

        # Delete the subscription
        delete_statement = (
            subscription_table.delete()
            .where(subscription_table.c.id == subscription_id)
            .where(subscription_table.c.despUserId == despUserId)
        )
        delete_result = await session.execute(delete_statement)
        if delete_result.rowcount == 0:
            # This should not happen since we checked existence, but handle it just in case
            raise DespGenericError(
                status_code=404, code=SUBSCRIPTION_NOT_FOUND_ERROR, message=SUBSCRIPTION_NOT_FOUND_MESSAGE
            )

        # Commit is handled by the context manager
    except SQLAlchemyError as sae:
        message = "Failed to query the database to delete the subscription"
        logger.exception(message, exc_info=sae)
        raise DespGenericError(status_code=500, code=SUBSCRIPTION_DELETE_FAILED, message=message) from sae


async def check_subscription_and_send_mails(
    asset: SearchableDocument, subscriptions: list[Subscription], current_metadatas: list[Metadata]
) -> None:
    """Check if the asset matches each subscription and send the mails to the users."""
    try:
        valid_subscriptions: list[Subscription] = await check_subscriptions_matches(
            asset, subscriptions, current_metadatas
        )
        mails = await create_mails_from_subscriptions(valid_subscriptions, asset)
        logger.info("There are %s mails to send for the asset %s", len(mails), asset.name)
        if len(mails) == 0:
            return
        await send_all_subscriptions_notifications(mails)
    except Exception as e:
        logger.exception("Error while checking subscriptions and sending mails: %s", e)
        raise DespGenericError(status_code=500, code=SUBSCRIPTION_CHECK_FAILED, message=str(e)) from e


async def send_all_subscriptions_notifications(mails: list[SubscriptionMatchMail]) -> None:
    """Send all the mails to the users."""
    for mail in mails:
        logger.info("Sending mail to %s", mail.user_email)
        await send_email_to_mq(
            notification_type=NotificationTemplate.GENERIC,
            user_email=mail.user_email,
            subject=mail.subject,
            message=mail.message,
            user_id=mail.user_id,
        )


async def create_mails_from_subscriptions(
    subscriptions: list[Subscription], asset: SearchableDocument
) -> list[SubscriptionMatchMail]:
    """Get the mails from the subscriptions."""
    mails = []
    for s in subscriptions:
        try:
            mails.append(
                SubscriptionMatchMail(
                    user_email=await get_mail_from_desp_user_id(s.despUserId),
                    subject=f"New asset matching your subscription {s.name}",
                    message=(
                        f"Hello {s.despUserId},\n"
                        f"The asset {asset.name} has been created or updated and "
                        f"matches with your subscription {s.name}"
                    ),
                    user_id=s.despUserId,
                )
            )
        except Exception as e:
            logger.exception("Error while preparing mail for subscription '%s': %s", s.name, e)
    return mails


async def check_subscriptions_matches_text_query(
    asset: SearchableDocument, subscriptions: list[Subscription]
) -> list[Subscription]:
    """Filter the subscriptions that match the asset with their text query and return the filtered list."""
    return [s for s in subscriptions if s.query.text in asset.name]


async def check_subscription_matches_type(
    asset: SearchableDocument, subscriptions: list[Subscription]
) -> list[Subscription]:
    """Filter the subscriptions that match the asset type and return the filtered list."""
    return [s for s in subscriptions if s.query.documentType == asset.documentType or s.query.documentType is None]


async def check_subscription_matches_category(
    asset: SearchableDocument, subscriptions: list[Subscription]
) -> list[Subscription]:
    """Filter the subscriptions that match the asset category and return the filtered list."""
    subscriptions_without_category = [s for s in subscriptions if s.query.documentCategory is None]
    subscriptions_with_category = [s for s in subscriptions if s.query.documentCategory is not None]
    result_subscriptions = [
        s for s in subscriptions_with_category if any(asset.categoryId == c for c in s.query.documentCategory)
    ]
    result_subscriptions.extend(subscriptions_without_category)
    return result_subscriptions


async def check_subscription_matches_source(
    asset: SearchableDocument, subscriptions: list[Subscription]
) -> list[Subscription]:
    """Filter the subscriptions that match the asset source and return the filtered list."""
    return [s for s in subscriptions if (s.query.documentSource == asset.source or s.query.documentSource is None)]


async def check_subscription_matches_textual_metadata(
    asset: SearchableDocument, subscriptions: list[Subscription], metadatas: list[Metadata]
) -> list[Subscription]:
    """Filter the subscriptions that match the asset with their textual metadata, using "contains" operations.
    The asset metadata must contain the text specified in the subscription.
    """
    subscriptions_without_any_metadata = [s for s in subscriptions if s.query.metadatas is None]
    subscriptions = [s for s in subscriptions if s.query.metadatas is not None]

    # Filter text metadata : we check if the asset "contains" the required text from the subscription
    # We also keep the subscriptions that do not have this metadata as they do not care about it
    textual_metadatas = [m for m in metadatas if m.type in [MetadataType.string, MetadataType.text, MetadataType.code]]
    for m in textual_metadatas:
        subscriptions_without_m = [s for s in subscriptions if m.name not in s.query.metadatas.keys()]
        subscriptions_with_m = [s for s in subscriptions if m.name in s.query.metadatas.keys()]
        subscriptions_with_m = [s for s in subscriptions_with_m if s.query.metadatas[m.name] in asset.metadata[m.name]]
        subscriptions = subscriptions_without_m + subscriptions_with_m
        logger.debug(
            "After filtering by metadata '%s', %s subscriptions remaining",
            m.name,
            len(subscriptions_without_any_metadata) + len(subscriptions),
        )

    return subscriptions_without_any_metadata + subscriptions


async def check_subscription_matches_select_metadata(
    asset: SearchableDocument, subscriptions: list[Subscription], metadatas: list[Metadata]
) -> list[Subscription]:
    """Filter the subscriptions that match the asset with their select metadata, using "equals" operations.
    The asset metadata must equal the value specified in the subscription.
    """
    subscriptions_without_any_metadata = [s for s in subscriptions if s.query.metadatas is None]
    subscriptions = [s for s in subscriptions if s.query.metadatas is not None]

    # Filter select metadata : we check if the asset "equals" the required value from the subscription
    # We also keep the subscriptions that do not have this metadata as they do not care about it
    select_metadatas = [m for m in metadatas if m.type == MetadataType.select]
    for m in select_metadatas:
        subscriptions_without_m = [s for s in subscriptions if m.name not in s.query.metadatas.keys()]
        subscriptions_with_m = [s for s in subscriptions if m.name in s.query.metadatas.keys()]
        subscriptions_with_m = [s for s in subscriptions_with_m if asset.metadata[m.name] == s.query.metadatas[m.name]]
        subscriptions = subscriptions_without_m + subscriptions_with_m
        logger.debug(
            "After filtering by metadata '%s', %s subscriptions remaining",
            m.name,
            len(subscriptions_without_any_metadata) + len(subscriptions),
        )

    return subscriptions_without_any_metadata + subscriptions


async def check_subscriptions_matches(
    asset: SearchableDocument,
    subscriptions: list[Subscription],
    current_metadatas: list[Metadata],
) -> list[Subscription]:
    """Check if the asset matches each subscription.
    Returns a list of the subscriptions that match the asset.
    """
    logger.debug("Checking subscriptions matches for asset %s, %s subscriptions", asset.to_dict(), len(subscriptions))

    # Filter by documentType

    subscriptions = await check_subscription_matches_type(asset, subscriptions)

    logger.debug("After filtering by documentType, %s subscriptions", len(subscriptions))
    if len(subscriptions) == 0:
        logger.debug("No subscription matches the asset documentType")
        return []

    # Filter by categoryId

    subscriptions = await check_subscription_matches_category(asset, subscriptions)

    logger.debug("After filtering by categoryId, %s subscriptions", len(subscriptions))
    if len(subscriptions) == 0:
        logger.debug("No subscription matches the asset categoryId")
        return []

    # Filter by source

    subscriptions = await check_subscription_matches_source(asset, subscriptions)

    logger.debug("After filtering by source, %s subscriptions", len(subscriptions))
    if len(subscriptions) == 0:
        logger.debug("No subscription matches the asset source")
        return []

    # Filter by text query

    subscriptions = await check_subscriptions_matches_text_query(asset, subscriptions)

    logger.debug("After filtering by text query, %s subscriptions", len(subscriptions))
    if len(subscriptions) == 0:
        logger.debug("No subscription matches the asset text query")
        return []

    # Filter by metadata

    subscriptions = await check_subscription_matches_textual_metadata(asset, subscriptions, current_metadatas)
    subscriptions = await check_subscription_matches_select_metadata(asset, subscriptions, current_metadatas)

    logger.debug("After filtering by metadata, %s subscriptions", len(subscriptions))

    if len(subscriptions) == 0:
        logger.debug("No subscription matches the asset source")
        return []

    return subscriptions
