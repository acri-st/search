"""Declare the routes for the metadatas API.
Admin UI can do crud operations on metadatas.
"""

from uuid import UUID

from fastapi import APIRouter
from msfwk import database
from msfwk.application import openapi_extra
from msfwk.models import BaseDespResponse, DespResponse

from search.models.metadatas import Metadata, MetadataCreate, MetadataUpdate
from search.services.metadatas import (
    create_metadata_in_db,
    delete_metadata_in_db,
    get_metadatas_in_db,
    update_metadata_in_db,
)

router = APIRouter()


@router.post(
    "/metadatas",
    summary="Create a new metadata",
    tags=["metadatas"],
    response_model=BaseDespResponse[Metadata],
    openapi_extra=openapi_extra(secured=True, roles=["admin"], internal=True),
)
async def create_metadata(metadata: MetadataCreate) -> DespResponse[Metadata]:
    """Create a new metadata"""
    async with database.get_schema("collaborative").get_async_session() as db_session:
        result = await create_metadata_in_db(db_session, metadata)
        return DespResponse(data=result)


@router.get(
    "/metadatas",
    summary="Get all metadatas",
    tags=["metadatas"],
    response_model=BaseDespResponse[list[Metadata]],
    openapi_extra=openapi_extra(secured=False, roles=[]),
)
async def get_all_metadatas() -> DespResponse[list[Metadata]]:
    """Get all metadatas"""
    async with database.get_schema("collaborative").get_async_session() as db_session:
        result = await get_metadatas_in_db(db_session)
        return DespResponse(data=[metadata.model_dump() for metadata in result])


@router.get(
    "/metadatas/{metadata_id}",
    summary="Get a metadata by ID",
    tags=["metadatas"],
    response_model=BaseDespResponse[Metadata],
    openapi_extra=openapi_extra(secured=False, roles=[], internal=True),
)
async def get_metadata(metadata_id: UUID) -> DespResponse[Metadata]:
    """Get a metadata by ID"""
    async with database.get_schema("collaborative").get_async_session() as db_session:
        result = await get_metadatas_in_db(db_session, metadata_id=metadata_id)
        return DespResponse(data=result[0].model_dump())


@router.patch(
    "/metadatas/{metadata_id}",
    summary="Update a metadata and return the updated metadata",
    tags=["metadatas"],
    response_model=BaseDespResponse[Metadata],
    openapi_extra=openapi_extra(secured=True, roles=["admin"], internal=True),
)
async def update_metadata(metadata_id: UUID, update_data: MetadataUpdate) -> DespResponse[Metadata]:
    """Update a metadata"""
    async with database.get_schema("collaborative").get_async_session() as db_session:
        result = await update_metadata_in_db(db_session, metadata_id, update_data)
        return DespResponse(data=result.model_dump())


@router.delete(
    "/metadatas/{metadata_id}",
    summary="Delete a metadata",
    tags=["metadatas"],
    response_model=BaseDespResponse[None],
    openapi_extra=openapi_extra(secured=True, roles=["admin"], internal=True),
)
async def delete_metadata(metadata_id: UUID) -> DespResponse[None]:
    """Delete a metadata"""
    async with database.get_schema("collaborative").get_async_session() as db_session:
        async with db_session.begin():
            await delete_metadata_in_db(db_session, metadata_id)
        return DespResponse(data=None)
