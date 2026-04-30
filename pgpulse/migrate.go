package pgpulse

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
)

//go:embed schema.sql
var schema string

// Migrate runs the PgPulse schema migration against the given database.
// It is safe to call multiple times (uses IF NOT EXISTS).
func Migrate(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, schema)
	if err != nil {
		return fmt.Errorf("pgpulse: migrate: %w", err)
	}
	return nil
}
