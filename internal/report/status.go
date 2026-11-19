package report

// Status is the traffic light of a node, edge, chain or the whole report.
type Status string

const (
	StatusOK      Status = "ok"
	StatusUnknown Status = "unknown"
	StatusWarning Status = "warning"
	StatusError   Status = "error"
)

// rank orders statuses from best to worst: ok < unknown < warning < error (§9.3).
func (status Status) rank() int {
	switch status {
	case StatusUnknown:
		return 1
	case StatusWarning:
		return 2
	case StatusError:
		return 3
	default:
		return 0
	}
}

// Compare orders statuses from best to worst, for sorting: negative when status is better
// than other, zero when equal, positive when worse.
func (status Status) Compare(other Status) int {
	return status.rank() - other.rank()
}

// Worst returns the worst of the statuses, or ok when there are none.
func Worst(statuses ...Status) Status {
	worst := StatusOK
	for _, status := range statuses {
		if status.rank() > worst.rank() {
			worst = status
		}
	}

	return worst
}

// Severity is how bad a finding is. It is narrower than Status: a finding is never ok or unknown.
type Severity string

// Finding severities.
const (
	SeverityWarning Severity = "warning"
	SeverityError   Severity = "error"
)

// Status returns the status a finding of this severity gives to its subjects.
func (severity Severity) Status() Status {
	if severity == SeverityError {
		return StatusError
	}

	return StatusWarning
}

// Compare orders severities from best to worst, like Status.Compare.
func (severity Severity) Compare(other Severity) int {
	return severity.Status().Compare(other.Status())
}
