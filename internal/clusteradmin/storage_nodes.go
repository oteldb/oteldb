package clusteradmin

import "github.com/oteldb/oteldb/internal/adminapi"

func storageNodeReports(answers []nodeAnswer[*adminapi.StorageStats]) []adminapi.StorageNodeReport {
	statuses := nodeStatuses(answers)
	reports := make([]adminapi.StorageNodeReport, 0, len(answers))
	for i, answer := range answers {
		status := statuses[i]
		report := adminapi.StorageNodeReport{
			Node: status.Node, Addr: status.Addr, Status: status.Status,
			Error: status.Error, DurationSeconds: status.DurationSeconds,
		}
		if answer.ok() {
			report.Engine = answer.Value.Engine
		}
		reports = append(reports, report)
	}
	return reports
}

// Local health cannot be combined across replicas or inferred from an older peer's omission.
func clearNodeSignalState(s *adminapi.EngineSignalStats) {
	s.HasReadGap.Reset()
	s.ReadGapAfterUnixNano.Reset()
	s.OutOfSpace.Reset()
	s.WantedParts.Reset()
	s.Holes.Reset()
	s.LostParts.Reset()
	s.IndexFenced.Reset()
	s.IndexLoadError.Reset()
	s.TenureUnestablished.Reset()
}
