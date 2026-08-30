package clusteradmin

import (
	"context"
	"strings"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// GetHealth implements getHealth operation.
//
// The cluster's components are its nodes: one entry per member, carrying that node's own overall
// verdict. A node that did not answer is unhealthy rather than absent — dropping it would turn an
// unreachable node into a healthy cluster.
func (a *Aggregator) GetHealth(ctx context.Context) (*adminapi.HealthReport, error) {
	answers, err := fanout(ctx, a, "health",
		func(ctx context.Context, p Peer) (*adminapi.HealthReport, error) { return p.Client.GetHealth(ctx) },
	)
	if err != nil {
		return nil, err
	}

	report := &adminapi.HealthReport{
		Status:     adminapi.HealthStatusHealthy,
		Components: make([]adminapi.ComponentHealth, 0, len(answers)),
	}

	var healthy, total int
	for _, r := range answers {
		total++

		c := adminapi.ComponentHealth{Name: r.Peer.Node, Status: adminapi.HealthStatusHealthy}
		if r.Peer.Addr != "" {
			c.Addr = adminapi.NewOptString(r.Peer.Addr)
		}

		switch {
		case !r.ok():
			c.Status = adminapi.HealthStatusUnhealthy
			c.Error = adminapi.NewOptString(r.Err.Error())
		case r.Value.Status != adminapi.HealthStatusHealthy:
			c.Status = r.Value.Status
			c.Error = adminapi.NewOptString(unhealthyComponents(r.Value))
		default:
			healthy++
		}

		report.Components = append(report.Components, c)
	}

	switch {
	case total == 0:
		// An empty ring is not a healthy cluster: it answers every query with an empty result,
		// which reads as data rather than as an outage.
		report.Status = adminapi.HealthStatusUnhealthy
	case healthy == total:
		report.Status = adminapi.HealthStatusHealthy
	case healthy == 0:
		report.Status = adminapi.HealthStatusUnhealthy
	default:
		report.Status = adminapi.HealthStatusDegraded
	}

	return report, nil
}

// unhealthyComponents names what a node is unhappy about, so the cluster report says why a member
// is degraded without a second round trip.
func unhealthyComponents(r *adminapi.HealthReport) string {
	var names []string
	for _, c := range r.Components {
		if c.Status != adminapi.HealthStatusHealthy {
			names = append(names, c.Name)
		}
	}
	if len(names) == 0 {
		return string(r.Status)
	}

	return string(r.Status) + ": " + strings.Join(names, ", ")
}
