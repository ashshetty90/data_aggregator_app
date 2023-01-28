#!/usr/bin/env bash
docker rm data-aggregator-app
docker rmi -f data-aggregator-image
docker build --no-cache -t data-aggregator-image .
CID=$(docker run -d -it -v  --name data-aggregator-image)
docker logs -f $CID


