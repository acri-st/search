"""Manage the API entrypoints"""

from msfwk.application import app
from msfwk.context import current_config, register_destroy, register_init
from msfwk.mqclient import load_default_rabbitmq_config
from msfwk.utils.logging import get_logger

from search.routes.metadatas import router as metadata_router
from search.routes.search import router as search_router
from search.routes.subscriptions import router as subscription_router
from search.services.search import (
    destroy,
    init,
)

logger = get_logger("application")


async def init_mq(config: dict) -> bool:
    """Init"""
    logger.info("Initialising ...")
    load_succeded = load_default_rabbitmq_config()
    current_config.set(config)
    if load_succeded:
        logger.info("RabbitMQ config loaded")
    else:
        logger.error("Failed to load rabbitmq config")
    return load_succeded


app.include_router(subscription_router)
app.include_router(metadata_router)
app.include_router(search_router)


# Application Lifecycle
register_init(init_mq)
register_init(init)
register_destroy(destroy)
