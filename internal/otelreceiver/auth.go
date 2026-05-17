package otelreceiver

import (
	"context"
	"net/http"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensionauth"

	"github.com/go-faster/oteldb/internal/multitenancy"
)

type multitenancyAuthExtension struct {
	resolver multitenancy.Resolver
}

func (e *multitenancyAuthExtension) Start(context.Context, component.Host) error {
	return nil
}

func (e *multitenancyAuthExtension) Shutdown(context.Context) error {
	return nil
}

func (e *multitenancyAuthExtension) Authenticate(ctx context.Context, headers map[string][]string) (context.Context, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/", nil)
	for k, vs := range headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}

	decision, err := e.resolver.Resolve(ctx, req, multitenancy.OperationWrite)
	if err != nil {
		return ctx, err
	}

	return multitenancy.WithDecision(ctx, decision), nil
}

var multitenancyAuthType = component.MustNewType("multitenancyauth")

type multitenancyAuthConfig struct{}

func (c *multitenancyAuthConfig) Validate() error {
	return nil
}

func newMultitenancyAuthFactory(resolver multitenancy.Resolver) extension.Factory {
	return extension.NewFactory(
		multitenancyAuthType,
		func() component.Config {
			return &multitenancyAuthConfig{}
		},
		func(ctx context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
			return &multitenancyAuthExtension{
				resolver: resolver,
			}, nil
		},
		component.StabilityLevelDevelopment,
	)
}

var _ extensionauth.Server = (*multitenancyAuthExtension)(nil)
