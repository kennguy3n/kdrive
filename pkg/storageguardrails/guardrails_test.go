package wasabiguardrails_test

import (
	"testing"
	"time"

	"github.com/kchat/drive/pkg/wasabiguardrails"
)

func TestDefaultGuardrailsValidate(t *testing.T) {
	g := wasabiguardrails.DefaultGuardrails("tenant-1")
	if err := g.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestResidualBillableWindow(t *testing.T) {
	now := time.Now()
	tracker := wasabiguardrails.MinStorageTracker{
		TenantID:           "t1",
		PieceID:            "p1",
		StoredAt:           now,
		MinStorageDuration: 90 * 24 * time.Hour,
	}
	// Deleted immediately: residual is ~90 days.
	residual := tracker.ResidualBillableWindow(now)
	if residual <= 0 {
		t.Errorf("residual after immediate delete = %v, want > 0", residual)
	}
	// Deleted after 100 days: residual is 0.
	tracker.DeletedAt = now.Add(100 * 24 * time.Hour)
	if r := tracker.ResidualBillableWindow(now); r != 0 {
		t.Errorf("residual after 100-day delete = %v, want 0", r)
	}
}

func TestBudgetUsage(t *testing.T) {
	g := wasabiguardrails.DefaultGuardrails("t1")
	g.Budget.SoftCapBytes = 1000
	g.Budget.HardCapBytes = 2000

	// Below soft cap.
	usage := wasabiguardrails.BudgetUsage{Budget: g.Budget, UsedBytes: 800}
	if usage.SoftCapExceeded() {
		t.Errorf("SoftCapExceeded at 800 = true, want false")
	}
	if usage.HardCapExceeded() {
		t.Errorf("HardCapExceeded at 800 = true, want false")
	}

	// Above soft cap, below hard cap.
	usage.UsedBytes = 1500
	if !usage.SoftCapExceeded() {
		t.Errorf("SoftCapExceeded at 1500 = false, want true")
	}
	if usage.HardCapExceeded() {
		t.Errorf("HardCapExceeded at 1500 = true, want false")
	}

	// Above hard cap.
	usage.UsedBytes = 2100
	if !usage.SoftCapExceeded() {
		t.Errorf("SoftCapExceeded at 2100 = false, want true")
	}
	if !usage.HardCapExceeded() {
		t.Errorf("HardCapExceeded at 2100 = false, want true")
	}
}
