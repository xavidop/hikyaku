package http

import "github.com/xavidop/hikyaku/internal/egress"

// The SSRF guard of a destination whose URL host is a per-message template
// lives in internal/egress, which the enrich transform shares.
type guard = egress.Guard

var (
	newGuard  = egress.New
	errEgress = egress.ErrDenied
)
