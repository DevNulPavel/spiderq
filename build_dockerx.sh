#! /usr/bin/env bash

APP_VERSION=$(cargo metadata --format-version=1 | jq '.packages[] | select(.name=="spiderq") | .version' | tr -d '"')
echo "App version is $APP_VERSION"

DOCKER_REGISTRY=$1
echo "Docker registry is $DOCKER_REGISTRY"

if [ ! -z "$DOCKER_REGISTRY" ]; then
    DOCKER_REGISTRY=$DOCKER_REGISTRY/
fi

docker buildx build \
    --platform linux/amd64,linux/arm64 \
    --file ./Dockerfile \
    --tag ${DOCKER_REGISTRY}spiderq:$APP_VERSION \
    --tag ${DOCKER_REGISTRY}spiderq:latest \
    --label spiderq \
    .

# docker push ${DOCKER_REGISTRY}spiderq:$APP_VERSION
# docker push ${DOCKER_REGISTRY}spiderq:latest

# docker push --all-tags --quiet {DOCKER_REGISTRY}spiderq