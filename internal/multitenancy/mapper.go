package multitenancy

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/otel/metric"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// TenantMapper resolves tenant_id from resource attributes.
type TenantMapper struct {
	// explicit compound key → tenant_id map
	tenants map[string]tenantRule
	// template for dynamic tenant_id generation
	template string
	// default tenant when no match
	defaultTenant string
	// cache for resolved tenant IDs
	cache sync.Map // map[string]string (attribute hash → tenant_id)

	// metrics
	cacheHits   atomic.Int64
	cacheMisses atomic.Int64
	unmapped    atomic.Int64
}

type tenantRule struct {
	id            string
	keyAttributes map[string]string
}

// TenantMapperConfig configures a TenantMapper.
type TenantMapperConfig struct {
	// Tenants is an explicit list of compound key maps.
	// Each entry maps a set of resource attribute key-value pairs to a tenant ID.
	Tenants []TenantRule
	// TenantIDTemplate is a Go template string for dynamic tenant ID generation.
	// Example: "{example.org/app}-{example.org/env}"
	// This is mutually exclusive with Tenants; if both are set, Tenants takes precedence.
	TenantIDTemplate string
	// DefaultTenant is the fallback tenant ID when no rule matches.
	// If empty, unmapped records are dropped.
	DefaultTenant string
}

// TenantRule defines a compound key mapping.
type TenantRule struct {
	// ID is the resulting tenant_id.
	ID string
	// KeyAttributes is the set of resource attribute key-value pairs that uniquely identify this tenant.
	KeyAttributes map[string]string
}

// NewTenantMapper creates a new TenantMapper from config.
func NewTenantMapper(cfg TenantMapperConfig) *TenantMapper {
	m := &TenantMapper{
		tenants:       make(map[string]tenantRule),
		template:      cfg.TenantIDTemplate,
		defaultTenant: cfg.DefaultTenant,
	}

	// Build lookup map for explicit tenants
	for _, t := range cfg.Tenants {
		key := buildLookupKey(t.KeyAttributes)
		m.tenants[key] = tenantRule{
			id:            t.ID,
			keyAttributes: t.KeyAttributes,
		}
	}

	return m
}

// RegisterMetrics registers OpenTelemetry metrics for this mapper.
func (m *TenantMapper) RegisterMetrics(meter metric.Meter) error {
	_, err := meter.Int64ObservableCounter(
		"multitenancy.mapper.cache_hits",
		metric.WithDescription("Number of tenant mapper cache hits"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(m.cacheHits.Load())
			return nil
		}),
	)
	if err != nil {
		return err
	}

	_, err = meter.Int64ObservableCounter(
		"multitenancy.mapper.cache_misses",
		metric.WithDescription("Number of tenant mapper cache misses"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(m.cacheMisses.Load())
			return nil
		}),
	)
	if err != nil {
		return err
	}

	_, err = meter.Int64ObservableCounter(
		"multitenancy.mapper.unmapped_records",
		metric.WithDescription("Number of records with no matching tenant"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(m.unmapped.Load())
			return nil
		}),
	)
	if err != nil {
		return err
	}

	_, err = meter.Int64ObservableGauge(
		"multitenancy.mapper.cache_size",
		metric.WithDescription("Current number of entries in the tenant mapper cache"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			var size int64
			m.cache.Range(func(_, _ interface{}) bool {
				size++
				return true
			})
			o.Observe(size)
			return nil
		}),
	)
	return err
}

// Resolve returns the tenant_id for the given resource attributes.
// Returns ("", false) if no tenant matches and no default is configured.
func (m *TenantMapper) Resolve(resourceAttrs otelstorage.Attrs) (string, bool) {
	// Fast path: check cache first
	cacheKey := buildAttrCacheKey(resourceAttrs)
	if cached, ok := m.cache.Load(cacheKey); ok {
		m.cacheHits.Add(1)
		tenantID := cached.(string)
		if tenantID == "" {
			return "", false
		}
		return tenantID, true
	}

	m.cacheMisses.Add(1)

	var tenantID string
	var found bool

	// Try explicit tenant rules first
	if len(m.tenants) > 0 {
		tenantID, found = m.resolveExplicit(resourceAttrs)
	}

	// Fall back to template
	if !found && m.template != "" {
		tenantID, found = m.resolveTemplate(resourceAttrs)
	}

	// Apply default if still no match
	if !found {
		if m.defaultTenant != "" {
			tenantID = m.defaultTenant
			found = true
		} else {
			// No match and no default
			m.unmapped.Add(1)
		}
	}

	// Cache the result (even if empty)
	m.cache.Store(cacheKey, tenantID)

	return tenantID, found
}

// resolveExplicit tries to match resource attributes against explicit tenant rules.
func (m *TenantMapper) resolveExplicit(resourceAttrs otelstorage.Attrs) (string, bool) {
	// Build a lookup key from the resource attributes
	attrs := resourceAttrs.AsMap()
	
	// Try each tenant rule
	for _, rule := range m.tenants {
		if m.matchesRule(attrs, rule.keyAttributes) {
			return rule.id, true
		}
	}
	
	return "", false
}

// matchesRule checks if all key attributes match.
func (m *TenantMapper) matchesRule(attrs pcommon.Map, keyAttributes map[string]string) bool {
	for key, expectedValue := range keyAttributes {
		value, ok := attrs.Get(key)
		if !ok {
			return false
		}
		if value.AsString() != expectedValue {
			return false
		}
	}
	return true
}

// resolveTemplate evaluates the tenant ID template with resource attributes.
func (m *TenantMapper) resolveTemplate(resourceAttrs otelstorage.Attrs) (string, bool) {
	attrs := resourceAttrs.AsMap()
	result := m.template

	// Simple template substitution: replace {key} with attribute value
	// This is a lightweight implementation; a full template engine can be added later.
	start := 0
	for {
		idx := strings.Index(result[start:], "{")
		if idx == -1 {
			break
		}
		idx += start
		endIdx := strings.Index(result[idx:], "}")
		if endIdx == -1 {
			break
		}
		endIdx += idx

		attrKey := result[idx+1 : endIdx]
		value, ok := attrs.Get(attrKey)
		if !ok {
			// Missing attribute means template cannot be evaluated
			return "", false
		}

		result = result[:idx] + value.AsString() + result[endIdx+1:]
		start = idx + len(value.AsString())
	}

	// Verify the template was fully expanded (no remaining placeholders)
	if strings.Contains(result, "{") {
		return "", false
	}

	return result, true
}

// buildLookupKey creates a stable cache key from a set of key-value pairs.
func buildLookupKey(attrs map[string]string) string {
	if len(attrs) == 0 {
		return ""
	}

	// Sort keys for stable ordering
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(k)
		buf.WriteByte('=')
		buf.WriteString(attrs[k])
	}
	return buf.String()
}

// buildAttrCacheKey creates a stable cache key from resource attributes.
func buildAttrCacheKey(resourceAttrs otelstorage.Attrs) string {
	attrs := resourceAttrs.AsMap()
	if attrs.Len() == 0 {
		return ""
	}

	// Collect all key-value pairs
	pairs := make(map[string]string, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		pairs[k] = v.AsString()
		return true
	})

	return buildLookupKey(pairs)
}

// Stats returns diagnostic information about the mapper.
func (m *TenantMapper) Stats() MapperStats {
	var cacheSize int
	m.cache.Range(func(_, _ interface{}) bool {
		cacheSize++
		return true
	})

	return MapperStats{
		ExplicitRules: len(m.tenants),
		HasTemplate:   m.template != "",
		CacheSize:     cacheSize,
		DefaultTenant: m.defaultTenant,
	}
}

// MapperStats contains diagnostic information about a TenantMapper.
type MapperStats struct {
	ExplicitRules int
	HasTemplate   bool
	CacheSize     int
	DefaultTenant string
}

// String returns a human-readable summary.
func (s MapperStats) String() string {
	return fmt.Sprintf("TenantMapper(rules=%d template=%v cache=%d default=%q)",
		s.ExplicitRules, s.HasTemplate, s.CacheSize, s.DefaultTenant)
}
