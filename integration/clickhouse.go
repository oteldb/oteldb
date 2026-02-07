package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testcontainerslog "github.com/testcontainers/testcontainers-go/log"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"

	"github.com/go-faster/oteldb/internal/chstorage"
)

// SetupCHOptions defines options [SetupCH].
type SetupCHOptions struct {
	Name           string
	TablePrefix    string
	Networks       []string
	SkipMigrate    bool
	TracerProvider trace.TracerProvider
}

func (opts *SetupCHOptions) setDefaults() {
	if opts.TracerProvider == nil {
		opts.TracerProvider = nooptrace.NewTracerProvider()
	}
}

// SetupCH runs a new Clickhouse instance.
func SetupCH(t *testing.T, opts SetupCHOptions) (testcontainers.Container, chstorage.ClickHouseClient, chstorage.Tables) {
	t.Helper()
	ctx := t.Context()
	opts.setDefaults()

	chName := "oteldb-clickhouse"
	if n := opts.Name; n != "" {
		chName = fmt.Sprintf("oteldb-%s-clickhouse", n)
	}
	req := testcontainers.ContainerRequest{
		Name:         chName,
		Image:        "clickhouse/clickhouse-server:25.9",
		ExposedPorts: []string{"8123/tcp", "9000/tcp"},
		Env: map[string]string{
			"CLICKHOUSE_PASSWORD": "default",
		},
		Networks: opts.Networks,
	}
	chContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Logger:           testcontainerslog.TestLogger(t),
		Reuse:            true,
	})
	require.NoError(t, err, "container start")

	endpoint, err := chContainer.PortEndpoint(ctx, "9000", "")
	require.NoError(t, err, "container endpoint")

	clientOpts := ch.Options{
		Address:  endpoint,
		Database: "default",
		User:     "default",
		Password: "default",

		OpenTelemetryInstrumentation: true,
		TracerProvider:               opts.TracerProvider,
	}

	connectBackoff := backoff.NewExponentialBackOff()
	connectBackoff.InitialInterval = 2 * time.Second
	connectBackoff.MaxElapsedTime = time.Minute
	c, err := backoff.RetryWithData(func() (*chpool.Pool, error) {
		c, err := chpool.Dial(ctx, chpool.Options{
			ClientOptions: clientOpts,
		})
		if err != nil {
			return nil, errors.Wrap(err, "dial")
		}
		return c, nil
	}, connectBackoff)
	if err != nil {
		t.Fatal(err)
	}

	tables := chstorage.DefaultTables()
	if !opts.SkipMigrate {
		if err := tables.Each(func(name *string) error {
			old := *name
			*name = opts.TablePrefix + "_" + old
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		t.Logf("Test tables prefix: %s", opts.TablePrefix)

		m := chstorage.NewMigrator(c, chstorage.MigratorOptions{
			Tables:     tables,
			Cluster:    "{cluster}",
			Replicated: false,
		})
		require.NoError(t, m.Create(ctx))
	}

	return chContainer, c, tables
}
