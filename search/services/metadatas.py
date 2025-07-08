"""Service functions to manage subscriptions in the database"""

import uuid
from uuid import UUID

from msfwk import database
from msfwk.exceptions import DespGenericError
from msfwk.utils.logging import get_logger
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from search.models.constants import (
    DATABASE_ERROR,
    METADATA_CREATE_FAILED,
    METADATA_DELETE_FAILED,
    METADATA_NOT_FOUND_ERROR,
    METADATA_NOT_FOUND_MESSAGE,
    METADATA_UPDATE_FAILED,
)
from search.models.metadatas import Metadata, MetadataCreate, MetadataUpdate

logger = get_logger("search")


async def create_metadata_in_db(session: AsyncSession, metadata: MetadataCreate) -> Metadata:
    """Create a new metadata in the database"""
    try:
        logger.debug("Creating metadata %s", metadata)
        metadata_table = database.get_schema("collaborative").tables["MetadataSchemas"]
        metadata_id = uuid.uuid4()
        statement = metadata_table.insert().values(
            {
                "id": metadata_id,
                "priority": metadata.priority,
                "name": metadata.name,
                "label": metadata.label,
                "description": metadata.description,
                "type": metadata.type,
                "asset_type": metadata.asset_type,
                "options": metadata.options,
                "section": metadata.section,
                "required": metadata.required,
                "queryable": metadata.queryable,
                "validator": metadata.validator,
            }
        )

        logger.info(statement)
        await session.execute(statement)
        await session.commit()

        created_metadata = await get_metadatas_in_db(session, metadata_id=metadata_id)
        if len(created_metadata) == 0:
            raise DespGenericError(status_code=500, code=METADATA_CREATE_FAILED, message="Metadata not created")
        return created_metadata[0]

    except DespGenericError as de:
        message = "Failed to query the database to create the metadata"
        if de.code == METADATA_NOT_FOUND_ERROR:
            raise DespGenericError(status_code=500, code=METADATA_CREATE_FAILED, message=message) from de
        raise de

    except SQLAlchemyError as sae:
        message = "Failed to query the database to create the metadata"
        logger.exception(message, exc_info=sae)
        raise DespGenericError(status_code=500, code=METADATA_CREATE_FAILED, message=message) from sae


async def get_metadatas_in_db(session: AsyncSession, metadata_id: UUID | None = None) -> list[Metadata]:
    """Get all metadatas, unless a metadata_id is provided"""
    try:
        logger.debug("Getting metadatas with metadata_id=%s", metadata_id)
        metadata_table = database.get_schema("collaborative").tables["MetadataSchemas"]
        if metadata_id is None:
            # No filter, all metadatas
            statement = select(metadata_table)
        elif metadata_id is not None:
            # Filter by metadata id
            statement = select(metadata_table).where(metadata_table.c.id == metadata_id)
        result = await session.execute(statement)
        rows = result.fetchall()
        if len(rows) == 0 and metadata_id is not None:
            # specific metadata not found
            raise DespGenericError(status_code=404, code=METADATA_NOT_FOUND_ERROR, message=METADATA_NOT_FOUND_MESSAGE)
        # otherwise, return all metadatas matching the filter
        return [Metadata(**row._mapping) for row in rows]
    except SQLAlchemyError as sae:
        message = "Failed to query the database to fetch the metadatas"
        logger.exception(message, exc_info=sae)
        raise DespGenericError(status_code=500, code=DATABASE_ERROR, message=message) from sae


async def update_metadata_in_db(session: AsyncSession, metadata_id: UUID, update_data: MetadataUpdate) -> Metadata:
    """Update a metadata in the database"""
    try:
        update_fields = {k: v for k, v in update_data.model_dump(exclude_unset=True).items() if v is not None}
        logger.debug("Updating metadata %s with %s", metadata_id, update_fields)
        metadata_table = database.get_schema("collaborative").tables["MetadataSchemas"]
        statement = metadata_table.update().where(metadata_table.c.id == metadata_id).values(update_fields)
        logger.info(statement)
        await session.execute(statement)
        await session.commit()
        return (await get_metadatas_in_db(session, metadata_id=metadata_id))[0]
    except SQLAlchemyError as sae:
        message = "Failed to query the database to update the metadata"
        logger.exception(message, exc_info=sae)
        raise DespGenericError(status_code=500, code=METADATA_UPDATE_FAILED, message=message) from sae


async def delete_metadata_in_db(session: AsyncSession, metadata_id: UUID) -> None:
    """Delete a metadata in the database"""
    try:
        logger.debug("Deleting metadata %s", metadata_id)
        metadata_table = database.get_schema("collaborative").tables["MetadataSchemas"]
        # Check if metadata exists and get it in a single query
        statement = select(metadata_table).where(metadata_table.c.id == metadata_id)
        result = await session.execute(statement)
        if not result.fetchone():
            raise DespGenericError(status_code=404, code=METADATA_NOT_FOUND_ERROR, message=METADATA_NOT_FOUND_MESSAGE)

        # Delete the metadata
        delete_statement = metadata_table.delete().where(metadata_table.c.id == metadata_id)
        delete_result = await session.execute(delete_statement)
        if delete_result.rowcount == 0:
            # This should not happen since we checked existence, but handle it just in case
            raise DespGenericError(status_code=404, code=METADATA_NOT_FOUND_ERROR, message=METADATA_NOT_FOUND_MESSAGE)

        # Commit is handled by the context manager
    except SQLAlchemyError as sae:
        message = "Failed to query the database to delete the metadata"
        logger.exception(message, exc_info=sae)
        raise DespGenericError(status_code=500, code=METADATA_DELETE_FAILED, message=message) from sae
