# docker-logql

A simple Docker CLI plugin to run LogQL queries over docker container logs.

## Installation

1. Build `docker-logql` binary.
   - **NOTE**: `docker-` prefix is important, docker would not find plugin without it.
2. Add binary to [plugin directory](https://github.com/docker/cli/blob/34797d167891c11d2e10c1339b072166b77a3378/cli-plugins/manager/manager_unix.go#L5-L8)
   - `~/.docker/cli-plugins` for current user
   - `/usr/local/libexec/docker/cli-plugins` for system-wide installation

Or use `make install`, it would build and add plugin to `~/.docker/cli-plugins` directory.

```console
git clone --depth 1 https://github.com/oteldb/oteldb
cd oteldb/cmd/docker-logql
make install
```

## Query logs

```console
$ docker logql query --help

Usage:  docker logql query <logql>

Examples:
# Get logs from all containers.
docker logql query '{}'

# Get logs for last 24h from container "registry" that contains "info".
docker logql query --since=1d '{container="registry"} |= "info"'

Options:
      --color                             Enable color (default true)
  -c, --container                         Show container name (default true)
  -d, --direction string                  Direction of sorting (default "asc")
      --end lokiapi.LokiTime              End of query range, defaults to now (default now)
  -l, --limit int                         Limit result (default -1)
      --since start                       A duration used to calculate start relative to `end` (default 6h)
      --start lokiapi.LokiTime            Start of query range (default `end - since`)
      --step lokiapi.PrometheusDuration   Query resolution step
  -t, --timestamp                         Show timestamps (default true)
```
