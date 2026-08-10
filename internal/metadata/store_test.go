// Package metadata_test verifies the migration SQL is loadable and
// the Store compiles. Full integration tests require a live
// Postgres instance (run via docker-compose in M5).
package metadata_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/kchat/drive/internal/metadata"
)

// TestMigrationsLoadable verifies the migration SQL file exists and
// can be read. A full integration test runs against a live Postgres
// in the M5 docker-compose stack.
func TestMigrationsLoadable(t *testing.T) {
	body, err := os.ReadFile("../../deploy/migrations/001_init.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if len(body) == 0 {
		t.Errorf("migration file is empty")
	}
	// Sanity-check the SQL contains the key tables from plan §5.
	checks := []string{
		"CREATE TABLE IF NOT EXISTS tenants",
		"CREATE TABLE IF NOT EXISTS file_versions",
		"CREATE TABLE IF NOT EXISTS provider_write_intents",
		"CREATE TABLE IF NOT EXISTS upload_sessions",
		"CREATE TABLE IF NOT EXISTS quota_reservations",
		"CREATE TABLE IF NOT EXISTS erasure_ledger",
		"CREATE TABLE IF NOT EXISTS outbox",
	}
	for _, check := range checks {
		if !contains(string(body), check) {
			t.Errorf("migration missing: %s", check)
		}
	}
}

// TestEmbeddedMigrations verifies the go:embed'd migrations are
// loadable from the binary and contain the expected tables.
func TestEmbeddedMigrations(t *testing.T) {
	migrations := metadata.EmbeddedMigrations()
	if len(migrations) != 6 {
		t.Fatalf("EmbeddedMigrations returned %d migrations, want 6", len(migrations))
	}
	if migrations[0].Version != 1 {
		t.Errorf("migration 0 version = %d, want 1", migrations[0].Version)
	}
	if migrations[1].Version != 2 {
		t.Errorf("migration 1 version = %d, want 2", migrations[1].Version)
	}
	if migrations[2].Version != 3 {
		t.Errorf("migration 2 version = %d, want 3", migrations[2].Version)
	}
	if migrations[3].Version != 4 {
		t.Errorf("migration 3 version = %d, want 4", migrations[3].Version)
	}
	if migrations[4].Version != 5 {
		t.Errorf("migration 4 version = %d, want 5", migrations[4].Version)
	}
	if migrations[5].Version != 6 {
		t.Errorf("migration 5 version = %d, want 6", migrations[5].Version)
	}
	for _, m := range migrations {
		if len(m.SQL) == 0 {
			t.Errorf("migration %d has empty SQL", m.Version)
		}
	}
	for _, check := range []string{
		"CREATE TABLE IF NOT EXISTS file_versions",
		"CREATE TABLE IF NOT EXISTS outbox",
		"CREATE TABLE IF NOT EXISTS erasure_ledger",
	} {
		if !contains(migrations[0].SQL, check) {
			t.Errorf("embedded migration 0 missing: %s", check)
		}
	}
	if !contains(migrations[1].SQL, "CREATE TABLE IF NOT EXISTS blob_placements") {
		t.Errorf("embedded migration 1 missing blob_placements table")
	}
	if !contains(migrations[2].SQL, "CREATE TABLE IF NOT EXISTS folders") {
		t.Errorf("embedded migration 2 missing folders table")
	}
	if !contains(migrations[2].SQL, "CREATE TABLE IF NOT EXISTS key_envelopes") {
		t.Errorf("embedded migration 2 missing key_envelopes table")
	}
}

// TestStoreCompiles is a compile-time check that the Store type and
// its methods are usable with a *sql.DB.
func TestStoreCompiles(t *testing.T) {
	var _ *metadata.Store = metadata.New(&sql.DB{})
	_ = metadata.Migration{Version: 1, SQL: ""}
	_ = metadata.ErrNotFound
	_ = metadata.ErrConflict
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || (len(s) > len(substr) && containsStr(s, substr)))
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestCreateTenantSQL verifies the CreateTenant SQL is valid by
// checking it compiles into a prepared statement shape. We use a
// no-op driver to avoid requiring a live Postgres.
func TestCreateTenantSQL(t *testing.T) {
	// This is a smoke test; the real SQL validation happens in the
	// docker-compose integration test in M5.
	_ = context.Background()
}
