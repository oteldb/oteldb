package integration

import (
	"fmt"
	"net/url"
	"testing"

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

	dsn := (&url.URL{
		Scheme:   "clickhouse",
		Host:     endpoint,
		User:     url.UserPassword("default", "default"),
		RawQuery: "compression=zstd",
		Path:     "/default",
	}).String()

	c, err := chstorage.Dial(ctx, dsn, chstorage.DialOptions{TracerProvider: opts.TracerProvider})
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

		diff, err := m.Diff(ctx)
		require.NoError(t, err)

		var notOk bool
		for _, d := range diff {
			t.Logf("Table: %s, Status: %s", d.Table, d.Status.String())
			notOk = notOk || d.Status != chstorage.MigrationOK
		}
		if notOk {
			t.Fatalf("Table schema is wrong or changed after migration")
		}
	}

	return chContainer, c, tables
}
