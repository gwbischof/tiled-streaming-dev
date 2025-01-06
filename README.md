# Tiled streaming prototype
The purpose of this project is to figure out how to implement streaming for tiled, using fastapi, asyncio, and postgres.
See: https://github.com/bluesky/tiled/issues/437


## Setup on Mac with podman

```
# Start the container.
podman run --name streaming-test-postgres -p 5432:5432 -e POSTGRES_PASSWORD=secret -e POSTGRES_USER=postgres -e POSTGRES_DB=streaming-test-postgres -d postgres

# Check that postgres is working.
PGPASSWORD=secret psql --host localhost --port 5432 --user postgres
```

## TODO:
- Add support for sqlite backend
- Subprotocols for websocket connections
