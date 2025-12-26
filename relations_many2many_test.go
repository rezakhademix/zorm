package zorm

import (
	"context"
	"testing"
)

func TestRelations_Attach(t *testing.T) {
	db := setupRelDBExtended(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	user := &RelUserExtended{ID: 1} // Alice

	// Attach role 2 (Editor) again? No, let's create a new role "Viewer" (ID 3)
	_, err := db.Exec("INSERT INTO rel_roles (id, name) VALUES (3, 'Viewer')")
	if err != nil {
		t.Fatal(err)
	}

	// Attach Viewer (3) to Alice (1)
	err = New[RelUserExtended]().Attach(ctx, user, "Roles", []any{3}, nil)
	if err != nil {
		t.Fatalf("Attach failed: %v", err)
	}

	// Verify in DB
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM rel_role_user WHERE user_id = 1 AND role_id = 3").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1 association, got %d", count)
	}

	// Verify via Load
	users, err := New[RelUserExtended]().With("Roles").Where("id", 1).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(users[0].Roles) != 3 { // Admin, Editor, Viewer
		t.Errorf("expected 3 roles, got %d", len(users[0].Roles))
	}
}

func TestRelations_Detach(t *testing.T) {
	db := setupRelDBExtended(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	user := &RelUserExtended{ID: 1} // Alice has Admin(1) and Editor(2)

	// Detach Editor (2)
	err := New[RelUserExtended]().Detach(ctx, user, "Roles", []any{2})
	if err != nil {
		t.Fatalf("Detach failed: %v", err)
	}

	// Verify in DB
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM rel_role_user WHERE user_id = 1 AND role_id = 2").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("expected 0 association for role 2, got %d", count)
	}

	// Verify remaining is Admin(1)
	err = db.QueryRow("SELECT COUNT(*) FROM rel_role_user WHERE user_id = 1 AND role_id = 1").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1 association for role 1, got %d", count)
	}
}

func TestRelations_DetachAll(t *testing.T) {
	db := setupRelDBExtended(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	user := &RelUserExtended{ID: 1} // Alice

	// Detach All
	err := New[RelUserExtended]().Detach(ctx, user, "Roles", nil)
	if err != nil {
		t.Fatalf("Detach all failed: %v", err)
	}

	// Verify
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM rel_role_user WHERE user_id = 1").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("expected 0 associations, got %d", count)
	}
}

func TestRelations_Sync(t *testing.T) {
	db := setupRelDBExtended(t)
	defer db.Close()

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	user := &RelUserExtended{ID: 1} // Alice has Admin(1) and Editor(2)

	// Add new roles
	_, err := db.Exec("INSERT INTO rel_roles (id, name) VALUES (3, 'Viewer'), (4, 'Guest')")
	if err != nil {
		t.Fatal(err)
	}

	// Sync: Keep Admin(1), Remove Editor(2), Add Viewer(3). Guest(4) ignored.
	// Target IDs: [1, 3]
	err = New[RelUserExtended]().Sync(ctx, user, "Roles", []any{1, 3}, nil)
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Verify in DB
	rows, err := db.Query("SELECT role_id FROM rel_role_user WHERE user_id = 1")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var roleIDs []int
	for rows.Next() {
		var id int
		rows.Scan(&id)
		roleIDs = append(roleIDs, id)
	}

	if len(roleIDs) != 2 {
		t.Errorf("expected 2 roles, got %d (%v)", len(roleIDs), roleIDs)
	}

	// Check content (order doesn't matter, but map check is easier)
	has1 := false
	has3 := false
	for _, id := range roleIDs {
		if id == 1 {
			has1 = true
		}
		if id == 3 {
			has3 = true
		}
	}

	if !has1 || !has3 {
		t.Errorf("expected roles [1, 3], got %v", roleIDs)
	}
}

func TestRelations_AttachWithPivotData(t *testing.T) {
	// Need a model with pivot columns?
	// RelRoleUser table in setupRelDBExtended only has user_id and role_id.
	// We can alter it for this test.
	db := setupRelDBExtended(t)
	defer db.Close()

	_, err := db.Exec("ALTER TABLE rel_role_user ADD COLUMN is_active BOOLEAN")
	if err != nil {
		t.Fatal(err)
	}

	oldDB := GlobalDB
	GlobalDB = db
	defer func() { GlobalDB = oldDB }()

	ctx := context.Background()
	user := &RelUserExtended{ID: 1}

	_, err = db.Exec("INSERT INTO rel_roles (id, name) VALUES (3, 'Viewer')")
	if err != nil {
		t.Fatal(err)
	}

	// Attach with pivot data
	pivotData := map[any]map[string]any{
		3: {"is_active": true},
	}

	err = New[RelUserExtended]().Attach(ctx, user, "Roles", []any{3}, pivotData)
	if err != nil {
		t.Fatalf("Attach with pivot failed: %v", err)
	}

	// Verify
	var isActive bool
	err = db.QueryRow("SELECT is_active FROM rel_role_user WHERE user_id = 1 AND role_id = 3").Scan(&isActive)
	if err != nil {
		t.Fatal(err)
	}

	if !isActive {
		t.Error("expected is_active to be true")
	}
}
