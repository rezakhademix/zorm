package zorm

import (
	"database/sql"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// DBConfig configures the connection pool settings.
type DBConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// ConnectPostgres creates a new *sql.DB connection pool for PostgreSQL using pgx driver.
// dsn: "postgres://user:password@host:port/dbname?sslmode=disable"
func ConnectPostgres(dsn string, config *DBConfig) (*sql.DB, error) {
	return connectPostgresWithOpener(dsn, config, sql.Open)
}

// Separate pool startup from driver registration so opening failures can be
// tested without replacing process-wide database drivers.
func connectPostgresWithOpener(dsn string, config *DBConfig, open func(string, string) (*sql.DB, error)) (*sql.DB, error) {
	db, err := open("pgx", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}

	if config != nil {
		if config.MaxOpenConns > 0 {
			db.SetMaxOpenConns(config.MaxOpenConns)
		}
		if config.MaxIdleConns > 0 {
			db.SetMaxIdleConns(config.MaxIdleConns)
		}
		if config.ConnMaxLifetime > 0 {
			db.SetConnMaxLifetime(config.ConnMaxLifetime)
		}
		if config.ConnMaxIdleTime > 0 {
			db.SetConnMaxIdleTime(config.ConnMaxIdleTime)
		}
	}

	return db, nil
}
