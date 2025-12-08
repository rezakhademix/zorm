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
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
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
