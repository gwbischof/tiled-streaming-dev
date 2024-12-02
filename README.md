## Setup on Mac with podman

```
# Start the container.
podman run --name streaming-test-postgres -p 5432:5432 -e POSTGRES_PASSWORD=secret -e POSTGRES_USER=postgres -e POSTGRES_DB=streaming-test-postgres -d postgres

# Check that postgres is working.
PGPASSWORD=secret psql --host localhost --port 5432 --user postgres
```
