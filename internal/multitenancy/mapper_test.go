package multitenancy

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

func makeAttrs(kvs map[string]string) otelstorage.Attrs {
	m := pcommon.NewMap()
	for k, v := range kvs {
		m.PutStr(k, v)
	}
	return otelstorage.Attrs(m)
}

func TestTenantMapper_ExplicitRules(t *testing.T) {
	mapper := NewTenantMapper(TenantMapperConfig{
		Tenants: []TenantRule{
			{
				ID: "fooapp-prod",
				KeyAttributes: map[string]string{
					"app": "foo",
					"env": "prod",
				},
			},
			{
				ID: "fooapp-dev",
				KeyAttributes: map[string]string{
					"app": "foo",
					"env": "dev",
				},
			},
			{
				ID: "barapp-prod",
				KeyAttributes: map[string]string{
					"app": "bar",
					"env": "prod",
				},
			},
		},
	})

	tests := []struct {
		name       string
		attrs      map[string]string
		wantTenant string
		wantOK     bool
	}{
		{
			name:       "exact match fooapp-prod",
			attrs:      map[string]string{"app": "foo", "env": "prod"},
			wantTenant: "fooapp-prod",
			wantOK:     true,
		},
		{
			name:       "exact match fooapp-dev",
			attrs:      map[string]string{"app": "foo", "env": "dev"},
			wantTenant: "fooapp-dev",
			wantOK:     true,
		},
		{
			name:       "extra attributes still match",
			attrs:      map[string]string{"app": "foo", "env": "prod", "region": "us-east"},
			wantTenant: "fooapp-prod",
			wantOK:     true,
		},
		{
			name:       "missing required attribute",
			attrs:      map[string]string{"app": "foo"},
			wantTenant: "",
			wantOK:     false,
		},
		{
			name:       "wrong value",
			attrs:      map[string]string{"app": "foo", "env": "staging"},
			wantTenant: "",
			wantOK:     false,
		},
		{
			name:       "no attributes",
			attrs:      map[string]string{},
			wantTenant: "",
			wantOK:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := makeAttrs(tt.attrs)
			tenant, ok := mapper.Resolve(attrs)
			if ok != tt.wantOK {
				t.Errorf("Resolve() ok = %v, want %v", ok, tt.wantOK)
			}
			if tenant != tt.wantTenant {
				t.Errorf("Resolve() tenant = %v, want %v", tenant, tt.wantTenant)
			}
		})
	}
}

func TestTenantMapper_Template(t *testing.T) {
	mapper := NewTenantMapper(TenantMapperConfig{
		TenantIDTemplate: "{app}-{env}",
	})

	tests := []struct {
		name       string
		attrs      map[string]string
		wantTenant string
		wantOK     bool
	}{
		{
			name:       "template expansion",
			attrs:      map[string]string{"app": "myapp", "env": "prod"},
			wantTenant: "myapp-prod",
			wantOK:     true,
		},
		{
			name:       "template with different values",
			attrs:      map[string]string{"app": "otherapp", "env": "staging"},
			wantTenant: "otherapp-staging",
			wantOK:     true,
		},
		{
			name:       "missing template key",
			attrs:      map[string]string{"app": "myapp"},
			wantTenant: "",
			wantOK:     false,
		},
		{
			name:       "extra attributes OK",
			attrs:      map[string]string{"app": "myapp", "env": "prod", "region": "us"},
			wantTenant: "myapp-prod",
			wantOK:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := makeAttrs(tt.attrs)
			tenant, ok := mapper.Resolve(attrs)
			if ok != tt.wantOK {
				t.Errorf("Resolve() ok = %v, want %v", ok, tt.wantOK)
			}
			if tenant != tt.wantTenant {
				t.Errorf("Resolve() tenant = %v, want %v", tenant, tt.wantTenant)
			}
		})
	}
}

func TestTenantMapper_DefaultTenant(t *testing.T) {
	mapper := NewTenantMapper(TenantMapperConfig{
		Tenants: []TenantRule{
			{
				ID: "known-tenant",
				KeyAttributes: map[string]string{
					"app": "known",
				},
			},
		},
		DefaultTenant: "default",
	})

	tests := []struct {
		name       string
		attrs      map[string]string
		wantTenant string
		wantOK     bool
	}{
		{
			name:       "known tenant",
			attrs:      map[string]string{"app": "known"},
			wantTenant: "known-tenant",
			wantOK:     true,
		},
		{
			name:       "unknown tenant uses default",
			attrs:      map[string]string{"app": "unknown"},
			wantTenant: "default",
			wantOK:     true,
		},
		{
			name:       "empty attributes uses default",
			attrs:      map[string]string{},
			wantTenant: "default",
			wantOK:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := makeAttrs(tt.attrs)
			tenant, ok := mapper.Resolve(attrs)
			if ok != tt.wantOK {
				t.Errorf("Resolve() ok = %v, want %v", ok, tt.wantOK)
			}
			if tenant != tt.wantTenant {
				t.Errorf("Resolve() tenant = %v, want %v", tenant, tt.wantTenant)
			}
		})
	}
}

func TestTenantMapper_Caching(t *testing.T) {
	mapper := NewTenantMapper(TenantMapperConfig{
		Tenants: []TenantRule{
			{
				ID: "cached-tenant",
				KeyAttributes: map[string]string{
					"app": "myapp",
				},
			},
		},
	})

	attrs := makeAttrs(map[string]string{"app": "myapp"})

	// First resolution - cache miss
	tenant1, ok1 := mapper.Resolve(attrs)
	if !ok1 || tenant1 != "cached-tenant" {
		t.Fatalf("First resolve failed: tenant=%v ok=%v", tenant1, ok1)
	}

	if mapper.cacheMisses.Load() != 1 {
		t.Errorf("Expected 1 cache miss, got %d", mapper.cacheMisses.Load())
	}
	if mapper.cacheHits.Load() != 0 {
		t.Errorf("Expected 0 cache hits, got %d", mapper.cacheHits.Load())
	}

	// Second resolution - cache hit
	tenant2, ok2 := mapper.Resolve(attrs)
	if !ok2 || tenant2 != "cached-tenant" {
		t.Fatalf("Second resolve failed: tenant=%v ok=%v", tenant2, ok2)
	}

	if mapper.cacheMisses.Load() != 1 {
		t.Errorf("Cache misses should still be 1, got %d", mapper.cacheMisses.Load())
	}
	if mapper.cacheHits.Load() != 1 {
		t.Errorf("Expected 1 cache hit, got %d", mapper.cacheHits.Load())
	}
}

func TestTenantMapper_UnmappedMetric(t *testing.T) {
	mapper := NewTenantMapper(TenantMapperConfig{
		Tenants: []TenantRule{
			{
				ID: "known",
				KeyAttributes: map[string]string{
					"app": "known",
				},
			},
		},
		// No default tenant
	})

	attrs := makeAttrs(map[string]string{"app": "unknown"})
	tenant, ok := mapper.Resolve(attrs)

	if ok {
		t.Errorf("Expected unmapped resolution to fail, got tenant=%v", tenant)
	}

	if mapper.unmapped.Load() != 1 {
		t.Errorf("Expected 1 unmapped record, got %d", mapper.unmapped.Load())
	}
}
