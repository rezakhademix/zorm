package zorm

import (
	"fmt"
	"testing"
)

func TestRegisterDriverDialect(t *testing.T) {
	db, _ := newRecordingDB()
	defer db.Close()
	originalDialect := GetDialect()
	SetDialect(DialectAuto)
	defer SetDialect(originalDialect)

	name := fmt.Sprintf("%T", db.Driver())
	driverDialectMu.RLock()
	originalRegistration := driverDialectRegistry[name]
	driverDialectMu.RUnlock()
	defer RegisterDriverDialect(name, originalRegistration)

	RegisterDriverDialect(name, DialectSQLite)
	if got := detectDialect(db); got != DialectSQLite {
		t.Errorf("expected registered SQLite dialect, got %v", got)
	}
	SetDialect(DialectPostgres)
	if got := detectDialect(db); got != DialectPostgres {
		t.Errorf("package override should take precedence, got %v", got)
	}
	SetDialect(DialectAuto)
	RegisterDriverDialect(name, DialectPostgres)
	if got := detectDialect(db); got != DialectPostgres {
		t.Errorf("expected updated PostgreSQL registration, got %v", got)
	}
	RegisterDriverDialect(name, DialectAuto)
	driverDialectMu.RLock()
	_, registered := driverDialectRegistry[name]
	driverDialectMu.RUnlock()
	if registered {
		t.Error("DialectAuto should remove the driver registration")
	}
	if got := detectDialect(db); got != DialectPostgres {
		t.Errorf("unknown driver should fall back to PostgreSQL, got %v", got)
	}
}
