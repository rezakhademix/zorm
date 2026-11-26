package zorm

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/stdlib"
)

// postgresPipeDSN uses pgx's real driver and protocol over an in-memory connection.
// No PostgreSQL process, listening socket, or network access is required.
func postgresPipeDSN(t *testing.T, rejectPing bool) string {
	t.Helper()
	config, err := pgx.ParseConfig("postgres://test@127.0.0.1/test?sslmode=disable&connect_timeout=2")
	if err != nil {
		t.Fatal(err)
	}
	config.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			_ = server.SetDeadline(time.Now().Add(5 * time.Second))
			backend := pgproto3.NewBackend(server, server)
			if _, err := backend.ReceiveStartupMessage(); err != nil {
				return
			}
			backend.Send(&pgproto3.AuthenticationOk{})
			backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "18.1"})
			backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if err := backend.Flush(); err != nil {
				return
			}
			for {
				message, err := backend.Receive()
				if err != nil {
					return
				}
				switch message.(type) {
				case *pgproto3.Terminate:
					return
				case *pgproto3.Query:
					if rejectPing {
						backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "ping rejected"})
					} else {
						backend.Send(&pgproto3.EmptyQueryResponse{})
					}
					backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					if err := backend.Flush(); err != nil {
						return
					}
				}
			}
		}()
		return client, nil
	}
	dsn := stdlib.RegisterConnConfig(config)
	t.Cleanup(func() { stdlib.UnregisterConnConfig(dsn) })
	return dsn
}

func TestConnectPostgres_PoolConfiguration(t *testing.T) {
	for _, config := range []*DBConfig{
		nil,
		{},
		{MaxOpenConns: 3, MaxIdleConns: 2, ConnMaxLifetime: time.Hour, ConnMaxIdleTime: time.Minute},
	} {
		db, err := ConnectPostgres(postgresPipeDSN(t, false), config)
		if err != nil {
			t.Fatalf("connection failed: %v", err)
		}
		if err := db.Ping(); err != nil {
			t.Errorf("returned pool is not usable: %v", err)
		}
		if config != nil && config.MaxOpenConns > 0 && db.Stats().MaxOpenConnections != config.MaxOpenConns {
			t.Errorf("max open connections not configured: %+v", db.Stats())
		}
		db.Close()
	}
}

func TestConnectPostgres_PingFailure(t *testing.T) {
	if db, err := ConnectPostgres(postgresPipeDSN(t, true), nil); err == nil || db != nil {
		if db != nil {
			db.Close()
		}
		t.Fatalf("failed ping returned a usable pool: db=%v err=%v", db, err)
	}
}

func TestConnectPostgres_OpeningFailureAndPingCleanup(t *testing.T) {
	if db, err := connectPostgresWithOpener("dsn", nil, func(driver, dsn string) (*sql.DB, error) {
		if driver != "pgx" || dsn != "dsn" {
			t.Errorf("wrong open arguments: %q %q", driver, dsn)
		}
		return nil, errDatabaseFailure
	}); db != nil || !errors.Is(err, errDatabaseFailure) {
		t.Errorf("opening failure lost: db=%v err=%v", db, err)
	}
	closed := 0
	pool := openFailureDB(t, &failureDB{pingErr: errDatabaseFailure, onClose: func() { closed++ }})
	db, err := connectPostgresWithOpener("dsn", nil, func(string, string) (*sql.DB, error) { return pool, nil })
	if db != nil || !errors.Is(err, errDatabaseFailure) {
		t.Errorf("ping failure lost: db=%v err=%v", db, err)
	}
	if closed != 1 {
		t.Errorf("failed startup leaked the opened connection: closes=%d", closed)
	}
}
