// Package wasabiguardrails defines the fair-use guardrail types the
// data plane uses to keep Wasabi usage inside its advertised envelope.
//
// The guardrails are declarative: the types describe the constraints
// and thresholds; the enforcement loop lives in the gateway's
// cache/billing modules and the drive-worker.
//
// The fair-use constraint that drives every rule below is:
//
//	per-tenant monthly Wasabi origin egress <= per-tenant active
//	storage volume on Wasabi.
//
// See KChat-Privacy-Drive-Storage-Architecture-v1.0.md §15.6/§15.7
// and the simplified plan §4.
package wasabiguardrails

import (
	"fmt"
	"time"
)

// WasabiMinStorageDays is Wasabi's 90-day minimum storage duration.
const WasabiMinStorageDays = 90

// WasabiStorageUSDPerTBMonth is Wasabi's headline storage price per
// TB-month. A "TB" here is 1e12 bytes and a "month" is 30 days.
const WasabiStorageUSDPerTBMonth = 6.99

var minStorageDuration = WasabiMinStorageDays * 24 * time.Hour

// FairUseEgressBudget describes the monthly Wasabi origin egress
// allowance for a single tenant. Wasabi's fair-use policy is
// interpreted as a budget that replenishes at the start of each
// billing window, sized to the tenant's active stored bytes
// multiplied by EgressStorageRatio.
type FairUseEgressBudget struct {
	TenantID           string    `json:"tenant_id"`
	WindowStart        time.Time `json:"window_start"`
	WindowDuration     time.Duration `json:"window_duration"`
	EgressStorageRatio float64   `json:"egress_storage_ratio"`
	// SoftCapBytes is the first alert threshold. Crossing it emits a
	// warning but does not throttle traffic.
	SoftCapBytes uint64 `json:"soft_cap_bytes"`
	// HardCapBytes is the enforcement threshold. Crossing it causes
	// the gateway to throttle or reject Wasabi origin reads per the
	// ThrottlePolicy field on AlertThresholds.
	HardCapBytes uint64 `json:"hard_cap_bytes"`
}

// MinStorageTracker tracks per-piece age on Wasabi so the billing
// pipeline can charge the full 90-day minimum storage duration even
// when a piece is deleted early.
type MinStorageTracker struct {
	TenantID           string        `json:"tenant_id"`
	PieceID            string        `json:"piece_id"`
	StoredAt           time.Time     `json:"stored_at"`
	DeletedAt          time.Time     `json:"deleted_at,omitempty"`
	MinStorageDuration time.Duration `json:"min_storage_duration"`
}

// CacheHitRatioTarget defines the per-tenant cache-hit-ratio floor
// that keeps Wasabi origin egress inside FairUseEgressBudget.
type CacheHitRatioTarget struct {
	TenantID       string        `json:"tenant_id"`
	Min            float64       `json:"min"`
	WindowDuration time.Duration `json:"window_duration"`
}

// AlertThresholds groups the operational thresholds used by the
// alerting pipeline.
type AlertThresholds struct {
	EgressBudgetWarnRatio     float64 `json:"egress_budget_warn_ratio"`
	EgressBudgetCriticalRatio float64 `json:"egress_budget_critical_ratio"`
	CacheHitRatioAlertMin     float64 `json:"cache_hit_ratio_alert_min"`
	// ThrottlePolicy names the action taken when HardCapBytes is
	// exceeded. Known values: "reject_origin_reads",
	// "slowdown_origin_reads", "notify_only".
	ThrottlePolicy string `json:"throttle_policy"`
}

// Guardrails is the full per-tenant Wasabi guardrail configuration.
type Guardrails struct {
	Budget     FairUseEgressBudget `json:"budget"`
	HitRatio   CacheHitRatioTarget `json:"hit_ratio"`
	Thresholds AlertThresholds     `json:"thresholds"`
	MinStorage time.Duration       `json:"min_storage_duration"`
}

// DefaultGuardrails returns the default guardrails for tenantID.
func DefaultGuardrails(tenantID string) Guardrails {
	return Guardrails{
		Budget: FairUseEgressBudget{
			TenantID:           tenantID,
			WindowDuration:     30 * 24 * time.Hour,
			EgressStorageRatio: 1.0,
		},
		HitRatio: CacheHitRatioTarget{
			TenantID:       tenantID,
			Min:            0.9,
			WindowDuration: 30 * 24 * time.Hour,
		},
		Thresholds: AlertThresholds{
			EgressBudgetWarnRatio:     0.8,
			EgressBudgetCriticalRatio: 0.95,
			CacheHitRatioAlertMin:     0.85,
			ThrottlePolicy:            "slowdown_origin_reads",
		},
		MinStorage: minStorageDuration,
	}
}

// Validate performs structural checks on the guardrail configuration.
func (g Guardrails) Validate() error {
	if g.Budget.TenantID == "" {
		return fmt.Errorf("wasabiguardrails: budget.tenant_id is required")
	}
	if g.Budget.EgressStorageRatio <= 0 {
		return fmt.Errorf("wasabiguardrails: budget.egress_storage_ratio must be > 0")
	}
	if g.Budget.WindowDuration <= 0 {
		return fmt.Errorf("wasabiguardrails: budget.window_duration must be > 0")
	}
	if g.Budget.HardCapBytes != 0 && g.Budget.HardCapBytes < g.Budget.SoftCapBytes {
		return fmt.Errorf("wasabiguardrails: budget.hard_cap_bytes (%d) must be >= soft_cap_bytes (%d)", g.Budget.HardCapBytes, g.Budget.SoftCapBytes)
	}
	if g.HitRatio.Min < 0 || g.HitRatio.Min > 1 {
		return fmt.Errorf("wasabiguardrails: hit_ratio.min must be in [0, 1] (got %v)", g.HitRatio.Min)
	}
	if g.HitRatio.WindowDuration <= 0 {
		return fmt.Errorf("wasabiguardrails: hit_ratio.window_duration must be > 0")
	}
	if g.Thresholds.EgressBudgetWarnRatio < 0 || g.Thresholds.EgressBudgetWarnRatio > 1 {
		return fmt.Errorf("wasabiguardrails: thresholds.egress_budget_warn_ratio must be in [0, 1]")
	}
	if g.Thresholds.EgressBudgetCriticalRatio < 0 || g.Thresholds.EgressBudgetCriticalRatio > 1 {
		return fmt.Errorf("wasabiguardrails: thresholds.egress_budget_critical_ratio must be in [0, 1]")
	}
	if g.Thresholds.EgressBudgetCriticalRatio != 0 &&
		g.Thresholds.EgressBudgetWarnRatio > g.Thresholds.EgressBudgetCriticalRatio {
		return fmt.Errorf("wasabiguardrails: thresholds.egress_budget_warn_ratio (%v) must be <= critical_ratio (%v)",
			g.Thresholds.EgressBudgetWarnRatio, g.Thresholds.EgressBudgetCriticalRatio)
	}
	if g.MinStorage <= 0 {
		return fmt.Errorf("wasabiguardrails: min_storage_duration must be > 0")
	}
	return nil
}

// ResidualBillableWindow returns the remaining billable duration when
// a piece is deleted before the minimum storage window expires. It
// returns zero if the piece has already outlived the minimum.
func (t MinStorageTracker) ResidualBillableWindow(now time.Time) time.Duration {
	if t.MinStorageDuration <= 0 {
		return 0
	}
	deletedAt := t.DeletedAt
	if deletedAt.IsZero() {
		deletedAt = now
	}
	expiry := t.StoredAt.Add(t.MinStorageDuration)
	if !deletedAt.Before(expiry) {
		return 0
	}
	return expiry.Sub(deletedAt)
}

// BudgetUsage reports the current egress usage against a budget.
type BudgetUsage struct {
	Budget       FairUseEgressBudget
	UsedBytes    uint64
	WindowStart  time.Time
	AsOf         time.Time
}

// SoftCapExceeded returns true when usage has crossed the soft cap.
func (u BudgetUsage) SoftCapExceeded() bool {
	if u.Budget.SoftCapBytes == 0 {
		return false
	}
	return u.UsedBytes >= u.Budget.SoftCapBytes
}

// HardCapExceeded returns true when usage has crossed the hard cap.
func (u BudgetUsage) HardCapExceeded() bool {
	if u.Budget.HardCapBytes == 0 {
		return false
	}
	return u.UsedBytes >= u.Budget.HardCapBytes
}

// WarnRatio returns the fraction of the soft cap currently used.
func (u BudgetUsage) WarnRatio() float64 {
	if u.Budget.SoftCapBytes == 0 {
		return 0
	}
	return float64(u.UsedBytes) / float64(u.Budget.SoftCapBytes)
}

// CriticalRatio returns the fraction of the hard cap currently used.
func (u BudgetUsage) CriticalRatio() float64 {
	if u.Budget.HardCapBytes == 0 {
		return 0
	}
	return float64(u.UsedBytes) / float64(u.Budget.HardCapBytes)
}
