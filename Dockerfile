# This argument is used to build in the CI
ARG BASE_VERSION=v6.3.0
ARG REGISTRY=harbor.shared.acrist-services.com/dsy/desp-aas/
#FROM should stay with the simple name of the image for tilt build
FROM ${REGISTRY}ms-base-image:${BASE_VERSION}
ARG CI_COMMIT_SHORT_SHA=xxxxxx
ARG BUILD_VERSION=0.0.0
ARG PIP_TOKEN=none
ENV ENTRYPOINT=search \
    GIT_HASH=$CI_COMMIT_SHORT_SHA \
    VERSION=$BUILD_VERSION \
    UV_INDEX_DSY_PIP_PASSWORD=$PIP_TOKEN
# This copy change the owner, this is needed for Tilt to override during development
COPY --chown=$LOCAL_USER:$LOCAL_GROUP . .
# Install dependencies in addition to the parents ones
RUN uv sync
# DO NOT OVERRIDE THE ENTRYPOINT BUT USE THE ENTRYPOINT ENV VAR