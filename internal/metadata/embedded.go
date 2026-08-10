// Package metadata — embedded migrations. The SQL files in
// deploy/migrations/ are embedded into the binary so the gateway can
// auto-migrate on startup without an operator manually piping SQL.
package metadata

import _ "embed"

//go:embed migrations/001_init.sql
var migration001SQL string

//go:embed migrations/002_blob_placements.sql
var migration002SQL string

//go:embed migrations/003_drive_demo.sql
var migration003SQL string

//go:embed migrations/004_content_dedup.sql
var migration004SQL string

//go:embed migrations/005_kdrv1_upload_sessions.sql
var migration005SQL string

//go:embed migrations/006_content_chunks_composite_idx.sql
var migration006SQL string

// EmbeddedMigrations returns the full set of embedded migrations in
// order. Pass these to Store.Migrate to apply them.
func EmbeddedMigrations() []Migration {
	return []Migration{
		{Version: 1, SQL: migration001SQL},
		{Version: 2, SQL: migration002SQL},
		{Version: 3, SQL: migration003SQL},
		{Version: 4, SQL: migration004SQL},
		{Version: 5, SQL: migration005SQL},
		{Version: 6, SQL: migration006SQL},
	}
}
