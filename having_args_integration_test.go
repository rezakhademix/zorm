package zorm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

type havingArgsRole struct {
	Role string
}

func TestHavingArgs_QueryShapes(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()
	cases := []struct {
		name   string
		model  func(*Model[havingArgsRole])
		scalar func(*ScalarQuery[string])
		args   []any
		roles  []string
	}{
		{"having-only",
			func(q *Model[havingArgsRole]) { q.Having("COUNT(*) >= ?", 2) },
			func(q *ScalarQuery[string]) { q.Having("COUNT(*) >= ?", 2) },
			[]any{2}, []string{"admin", "user"}},
		{"where-only",
			func(q *Model[havingArgsRole]) { q.Where("active", 1) },
			func(q *ScalarQuery[string]) { q.Where("active", 1) },
			[]any{1}, []string{"admin", "user"}},
		{"literal-having",
			func(q *Model[havingArgsRole]) { q.Having("COUNT(*) >= 2").Where("active", 1) },
			func(q *ScalarQuery[string]) { q.Having("COUNT(*) >= 2").Where("active", 1) },
			[]any{1}, []string{"admin"}},
		{"map-where",
			func(q *Model[havingArgsRole]) {
				q.Having("SUM(age) >= ?", 55).Where(map[string]any{"age": 30, "active": 1})
			},
			func(q *ScalarQuery[string]) {
				q.Having("SUM(age) >= ?", 55).Where(map[string]any{"age": 30, "active": 1})
			},
			[]any{1, 30, 55}, []string{}},
		{"multiple-args-per-clause",
			func(q *Model[havingArgsRole]) {
				q.Having("SUM(age) BETWEEN ? AND ?", 55, 60).Where("age BETWEEN ? AND ?", 25, 35).Where("active", 1)
			},
			func(q *ScalarQuery[string]) {
				q.Having("SUM(age) BETWEEN ? AND ?", 55, 60).Where("age BETWEEN ? AND ?", 25, 35).Where("active", 1)
			},
			[]any{25, 35, 1, 55, 60}, []string{"admin"}},
		{"nil-having-arg",
			func(q *Model[havingArgsRole]) { q.Having("COUNT(*) >= COALESCE(?, 2)", nil).Where("active", 1) },
			func(q *ScalarQuery[string]) { q.Having("COUNT(*) >= COALESCE(?, 2)", nil).Where("active", 1) },
			[]any{1, nil}, []string{"admin"}},
		{"operator-inference",
			func(q *Model[havingArgsRole]) { q.Having("COUNT(*) >=", 2).Where("active", 1) },
			func(q *ScalarQuery[string]) { q.Having("COUNT(*) >=", 2).Where("active", 1) },
			[]any{1, 2}, []string{"admin"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model := New[havingArgsRole]().SetDB(db).Select("role").GroupBy("role").OrderBy("role", "ASC")
			scalar := Query[string]().SetDB(db).Table("users").Select("role").GroupBy("role").OrderBy("role", "ASC")
			tc.model(model)
			tc.scalar(scalar)
			for name, print := range map[string]func() (string, []any){"model": model.Print, "scalar": scalar.Print} {
				_, args := print()
				if !reflect.DeepEqual(args, tc.args) {
					t.Errorf("%s args = %v, want %v", name, args, tc.args)
				}
			}
			rows, err := model.Get(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			roles := make([]string, 0, len(rows))
			for _, row := range rows {
				roles = append(roles, row.Role)
			}
			if !reflect.DeepEqual(roles, tc.roles) {
				t.Errorf("model roles = %v, want %v", roles, tc.roles)
			}
			values, err := scalar.Get(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(values, tc.roles) {
				t.Errorf("scalar roles = %v, want %v", values, tc.roles)
			}
		})
	}
}

func TestHavingArgs_ModelTerminals(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()
	ctx := context.Background()
	for _, cached := range []bool{false, true} {
		name := "uncached"
		if cached {
			name = "cached"
		}
		t.Run(name, func(t *testing.T) {
			cache := NewStmtCache(16)
			defer cache.Close()
			q := New[havingArgsRole]().SetDB(db).Select("role").GroupBy("role").Having("COUNT(*) >= ?", 2).Where("active", 1)
			if cached {
				q.WithStmtCache(cache)
			}
			row, err := q.First(ctx)
			if err != nil || row.Role != "admin" {
				t.Fatalf("First = %v, %v", row, err)
			}
			values, err := q.Pluck(ctx, "role")
			if err != nil || !reflect.DeepEqual(values, []any{"admin"}) {
				t.Fatalf("Pluck = %v, %v", values, err)
			}
			count, err := q.Count(ctx)
			if err != nil || count != 1 {
				t.Fatalf("Count = %d, %v, want 1", count, err)
			}
			page, err := q.Paginate(ctx, 1, 10)
			if err != nil {
				t.Fatal(err)
			}
			if page.Total != 1 || len(page.Data) != 1 || page.Data[0].Role != "admin" {
				t.Fatalf("Paginate = %+v", page)
			}
			cursor, err := q.Cursor(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer cursor.Close()
			if !cursor.Next() {
				t.Fatalf("Cursor.Next = false: %v", cursor.Err())
			}
			row, err = cursor.Scan(ctx)
			if err != nil || row.Role != "admin" {
				t.Fatalf("Cursor.Scan = %v, %v", row, err)
			}
			if cursor.Next() || cursor.Err() != nil {
				t.Fatalf("unexpected extra row or cursor error: %v", cursor.Err())
			}
		})
	}
	scalar := Query[string]().SetDB(db).Table("users").Select("role").GroupBy("role").Having("COUNT(*) >= ?", 2).Where("active", 1)
	value, err := scalar.First(ctx)
	if err != nil || value != "admin" {
		t.Fatalf("scalar First = %q, %v", value, err)
	}
	// Scalar Count counts WHERE matches; it does not render GROUP BY or HAVING.
	count, err := scalar.Count(ctx)
	if err != nil || count != 3 {
		t.Fatalf("scalar Count = %d, %v, want 3", count, err)
	}
}

func TestHavingArgs_CTEAndNestedWhere(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()
	sub := New[havingArgsRole]().SetDB(db).Select("role").GroupBy("role").Having("SUM(age) >= ?", 55).
		Where(func(q *Model[havingArgsRole]) { q.Where("active", 1).Where("age", ">=", 25) })
	q := New[havingArgsRole]().SetDB(db).WithCTE("eligible", sub).Table("eligible").Select("role").GroupBy("role").
		Having("COUNT(*) >= ?", 1).Where("role", "admin")
	_, args := q.Print()
	if want := []any{1, 25, 55, "admin", 1}; !reflect.DeepEqual(args, want) {
		t.Fatalf("args = %v, want %v", args, want)
	}
	rows, err := q.Get(context.Background())
	if err != nil || len(rows) != 1 || rows[0].Role != "admin" {
		t.Fatalf("Get = %v, %v", rows, err)
	}
	count, err := q.Count(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("Count = %d, %v, want 1", count, err)
	}
}

func TestHavingArgs_CloneAndReset(t *testing.T) {
	model := New[havingArgsRole]().Having("COUNT(*) >= ?", 2).Where("active", 1)
	clone := model.Clone()
	clone.havingArgs[0] = 3
	clone.Having("SUM(age) >= ?", 55).Where("age", ">=", 25)
	_, original := model.Print()
	if want := []any{1, 2}; !reflect.DeepEqual(original, want) {
		t.Fatalf("model clone changed base: %v", original)
	}
	_, args := clone.Print()
	if want := []any{1, 25, 3, 55}; !reflect.DeepEqual(args, want) {
		t.Fatalf("clone args = %v, want %v", args, want)
	}
	model.reset()
	model.Where("role", "user")
	_, args = model.Print()
	if want := []any{"user"}; !reflect.DeepEqual(args, want) {
		t.Fatalf("reset retained HAVING args: %v", args)
	}

	scalar := Query[string]().Table("users").Select("role").Having("COUNT(*) >= ?", 2).Where("active", 1)
	scalarClone := scalar.Clone()
	scalarClone.havingArgs[0] = 3
	scalarClone.Having("SUM(age) >= ?", 55).Where("age", ">=", 25)
	_, original = scalar.Print()
	if want := []any{1, 2}; !reflect.DeepEqual(original, want) {
		t.Fatalf("scalar clone changed base: %v", original)
	}
	_, args = scalarClone.Print()
	if want := []any{1, 25, 3, 55}; !reflect.DeepEqual(args, want) {
		t.Fatalf("scalar clone args = %v, want %v", args, want)
	}
}

func TestHavingArgs_ErrorContextAndRaw(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()
	ctx := context.Background()
	model := New[havingArgsRole]().SetDB(db).Select("role").GroupBy("role").Having("MISSING_FN(role) >= ?", 55).Where("active", 1)
	scalar := Query[string]().SetDB(db).Table("users").Select("role").GroupBy("role").Having("MISSING_FN(role) >= ?", 55).Where("active", 1)
	_, modelErr := model.Get(ctx)
	_, scalarErr := scalar.Get(ctx)
	for _, err := range []error{modelErr, scalarErr} {
		var qe *QueryError
		if !errors.As(err, &qe) {
			t.Fatalf("expected QueryError, got %v", err)
		}
		if want := []any{1, 55}; !reflect.DeepEqual(qe.Args, want) {
			t.Fatalf("error args = %v, want %v", qe.Args, want)
		}
	}
	model.Raw("SELECT role FROM users WHERE active = ? GROUP BY role HAVING COUNT(*) >= ?", 1, 2)
	_, args := model.Print()
	if want := []any{1, 2}; !reflect.DeepEqual(args, want) {
		t.Fatalf("raw args polluted by builder: %v", args)
	}
	rows, err := model.Get(ctx)
	if err != nil || len(rows) != 1 || rows[0].Role != "admin" {
		t.Fatalf("raw Get = %v, %v", rows, err)
	}
}

func (havingArgsRole) TableName() string { return "users" }

// Different values matter: using 1 for both WHERE and HAVING hides swapped binds.
func TestHavingArgs_ClauseOrder(t *testing.T) {
	db := setupScalarTestDB(t)
	defer db.Close()
	ctx := context.Background()
	// All 24 permutations also exercise reversed order within each clause type.
	var orders [][4]int
	for a := range 4 {
		for b := range 4 {
			for c := range 4 {
				if a != b && a != c && b != c {
					orders = append(orders, [4]int{a, b, c, 6 - a - b - c})
				}
			}
		}
	}
	for _, order := range orders {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			model := New[havingArgsRole]().SetDB(db).Select("role").GroupBy("role").OrderBy("role", "ASC")
			scalar := Query[string]().SetDB(db).Table("users").Select("role").GroupBy("role").OrderBy("role", "ASC")
			var whereArgs, havingArgs []any
			for _, clause := range order {
				switch clause {
				case 0:
					model.Where("active", 1)
					scalar.Where("active", 1)
					whereArgs = append(whereArgs, 1)
				case 1:
					model.Where("age", ">=", 25)
					scalar.Where("age", ">=", 25)
					whereArgs = append(whereArgs, 25)
				case 2:
					model.Having("COUNT(*) >= ?", 2)
					scalar.Having("COUNT(*) >= ?", 2)
					havingArgs = append(havingArgs, 2)
				case 3:
					model.Having("SUM(age) >= ?", 55)
					scalar.Having("SUM(age) >= ?", 55)
					havingArgs = append(havingArgs, 55)
				}
			}
			wantArgs := append(whereArgs, havingArgs...)
			for name, print := range map[string]func() (string, []any){"model": model.Print, "scalar": scalar.Print} {
				query, args := print()
				if !reflect.DeepEqual(args, wantArgs) {
					t.Errorf("%s SQL %q: args = %v, want %v", name, query, args, wantArgs)
				}
			}
			rows, err := model.Get(ctx)
			if err != nil {
				t.Errorf("model Get: %v", err)
			} else if len(rows) != 1 || rows[0].Role != "admin" {
				t.Errorf("model Get = %v, want one admin", rows)
			}
			values, err := scalar.Get(ctx)
			if err != nil {
				t.Errorf("scalar Get: %v", err)
			} else if !reflect.DeepEqual(values, []string{"admin"}) {
				t.Errorf("scalar Get = %v, want [admin]", values)
			}
		})
	}
}
