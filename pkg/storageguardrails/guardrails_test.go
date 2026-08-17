package storageguardrails_test

import (
	"testing"
	"time"

	"github.com/kchat/drive/pkg/storageguardrails"
)

func TestDefaultGuardrailsValidate(t *testing.T) {
	g := storageguardrails.DefaultGuardrails("tenant-1")
	if err := g.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestResidualBillableWindow(t *testing.T) {
	now := time.Now()
	tracker := storageguardrails.MinStorageTracker{
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
	g := storageguardrails.DefaultGuardrails("t1")
	g.Budget.SoftCapBytes = 1000
	g.Budget.HardCapBytes = 2000

	// Below soft cap.
	usage := storageguardrails.BudgetUsage{Budget: g.Budget, UsedBytes: 800}
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

func TestProfileFor(t *testing.T) {
	tests := []struct {
		provider        string
		wantMinDays     int
		wantEgressRatio float64
	}{
		{"wasabi", 90, 1.0},
		{"s3", 0, 0},
		{"b2", 0, 10.0},
		{"unknown", 0, 0}, // defaults to s3
	}
	for _, tt := range tests {
		t.Run(tt.provider, func(t *testing.T) {
			p := storageguardrails.ProfileFor(tt.provider)
			if p.MinStorageDays != tt.wantMinDays {
				t.Errorf("MinStorageDays = %d, want %d", p.MinStorageDays, tt.wantMinDays)
			}
			if p.EgressRatio != tt.wantEgressRatio {
				t.Errorf("EgressRatio = %v, want %v", p.EgressRatio, tt.wantEgressRatio)
			}
		})
	}
}

func TestDefaultGuardrailsForProvider(t *testing.T) {
	// Wasabi: 90-day min storage.
	w := storageguardrails.DefaultGuardrailsForProvider("t1", "wasabi")
	if w.MinStorage != 90*24*time.Hour {
		t.Errorf("wasabi MinStorage = %v, want 90d", w.MinStorage)
	}
	// S3: no min storage.
	s := storageguardrails.DefaultGuardrailsForProvider("t1", "s3")
	if s.MinStorage != 0 {
		t.Errorf("s3 MinStorage = %v, want 0", s.MinStorage)
	}
	// B2: no min storage, high egress ratio.
	b := storageguardrails.DefaultGuardrailsForProvider("t1", "b2")
	if b.MinStorage != 0 {
		t.Errorf("b2 MinStorage = %v, want 0", b.MinStorage)
	}
	if b.Budget.EgressStorageRatio != 10.0 {
		t.Errorf("b2 EgressStorageRatio = %v, want 10.0", b.Budget.EgressStorageRatio)
	}
}
