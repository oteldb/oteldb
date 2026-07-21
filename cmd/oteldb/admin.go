package main

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"

	"github.com/oteldb/oteldb/internal/adminapi"
	"github.com/oteldb/oteldb/internal/adminhandler"
	"github.com/oteldb/oteldb/internal/cliversion"
)

// setupAdmin wires the admin panel API and its embedded web UI. It is always registered.
func (app *App) setupAdmin() error {
	cfg := app.cfg.Admin
	cfg.setDefaults()

	build, _ := cliversion.GetInfo("github.com/oteldb/oteldb")

	opts := adminhandler.Options{
		Info: adminhandler.BuildInfo{
			Version:   build.Version,
			Commit:    build.Commit,
			GoVersion: build.GoVersion,
		},
		StartTime:         app.startTime,
		ClickHouseEnabled: app.chClient != nil,
		Signals:           app.adminSignals(),
		Components:        app.adminComponents(),
	}
	if b := app.storageBackend; b != nil {
		opts.Engine = b
		opts.StorageBackend = app.cfg.Storage.Backend
		opts.Maintain = b.MaintainNow
	}
	if app.chClient != nil {
		opts.CHStorage = adminhandler.NewCHStorageStats(app.chClient)
	}

	handler := adminhandler.NewAdminAPI(opts)
	s, err := adminapi.NewServer(handler,
		adminapi.WithAttributes(attribute.String("oteldb.api", "admin")),
		adminapi.WithTracerProvider(app.telemetry.TracerProvider()),
		adminapi.WithMeterProvider(app.telemetry.MeterProvider()),
	)
	if err != nil {
		return errors.Wrap(err, "create admin server")
	}

	addOgen(app, "admin", s, cfg.Bind, cfg.Auth, adminhandler.UIMiddleware())
	return nil
}

// adminSignals reports per-signal backend configuration for the admin API.
func (app *App) adminSignals() []adminapi.SignalInfo {
	tempo := app.cfg.Tempo
	tempo.setDefaults()
	prom := app.cfg.Prometheus
	prom.setDefaults()
	loki := app.cfg.Loki
	loki.setDefaults()
	pyro := app.cfg.Pyroscope
	pyro.setDefaults()

	backend := func(v string) string {
		if v == "" {
			return "none"
		}
		return v
	}
	signal := func(s adminapi.Signal, backendCfg, bind string, queryable bool) adminapi.SignalInfo {
		info := adminapi.SignalInfo{
			Signal:    s,
			Backend:   backend(backendCfg),
			Queryable: queryable,
		}
		if queryable {
			info.Bind = adminapi.NewOptString(bind)
		}
		return info
	}
	return []adminapi.SignalInfo{
		signal(adminapi.SignalMetrics, app.cfg.MetricsBackend, prom.Bind, app.metricsQuerier != nil),
		signal(adminapi.SignalTraces, app.cfg.TracesBackend, tempo.Bind, app.traceQuerier != nil),
		signal(adminapi.SignalLogs, app.cfg.LogsBackend, loki.Bind, app.logQuerier != nil),
		signal(adminapi.SignalProfiles, app.cfg.ProfilesBackend, pyro.Bind, app.profileQuerier != nil),
	}
}

// adminComponents lists the wired services for the admin health report.
func (app *App) adminComponents() []adminhandler.Component {
	var components []adminhandler.Component
	if client := app.chClient; client != nil {
		components = append(components, adminhandler.Component{
			Name:  "clickhouse",
			Check: func(ctx context.Context) error { return client.Ping(ctx) },
		})
	}
	components = append(components, adminhandler.Component{Name: "otelcol"})

	tempo := app.cfg.Tempo
	tempo.setDefaults()
	prom := app.cfg.Prometheus
	prom.setDefaults()
	loki := app.cfg.Loki
	loki.setDefaults()
	pyro := app.cfg.Pyroscope
	pyro.setDefaults()

	if app.metricsQuerier != nil {
		components = append(components, adminhandler.Component{Name: "prom", Addr: prom.Bind})
	}
	if app.traceQuerier != nil {
		components = append(components, adminhandler.Component{Name: "tempo", Addr: tempo.Bind})
	}
	if app.logQuerier != nil {
		components = append(components, adminhandler.Component{Name: "loki", Addr: loki.Bind})
	}
	if app.profileQuerier != nil {
		components = append(components, adminhandler.Component{Name: "pyroscope", Addr: pyro.Bind})
	}
	return components
}
