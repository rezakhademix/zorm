package zorm

import (
	"context"
	"reflect"
	"testing"
)

type selfRelationNode struct {
	ID       int
	ParentID *int
	Children []*selfRelationNode
}

func (selfRelationNode) TableName() string { return "self_relation_nodes" }
func (selfRelationNode) ChildrenRelation() HasMany[selfRelationNode] {
	return HasMany[selfRelationNode]{ForeignKey: "parent_id"}
}

func TestWhereHas_SelfRelationUsesSeparateScopes(t *testing.T) {
	for _, table := range []string{"self_relation_nodes", "self_relation_nodes AS n"} {
		for _, filter := range []string{"exists", "filtered", "nested"} {
			t.Run(table+"/"+filter, func(t *testing.T) {
				db := setupSaveDB(t)
				defer db.Close()
				_, err := db.Exec(`CREATE TABLE self_relation_nodes (id INTEGER PRIMARY KEY, parent_id INTEGER);
					INSERT INTO self_relation_nodes VALUES (1, NULL), (2, 1), (3, 2), (4, NULL)`)
				if err != nil {
					t.Fatal(err)
				}
				q := New[selfRelationNode]().SetDB(db).Table(table)
				want := []int{1, 2}
				switch filter {
				case "exists":
					q.WhereHas("Children", nil)
				case "filtered":
					q.WhereHas("Children", func(child *Model[selfRelationNode]) { child.Where(child.TableName()+".id", 3) })
					want = []int{2}
				case "nested":
					q.WhereHas("Children", func(child *Model[selfRelationNode]) { child.WhereHas("Children", nil) })
					want = []int{1}
				}
				rows, err := q.OrderBy("id", "ASC").Get(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				ids := make([]int, len(rows))
				for i, row := range rows {
					ids[i] = row.ID
					defer ClearOriginals(row)
				}
				if !reflect.DeepEqual(ids, want) {
					t.Errorf("self WhereHas returned %v, want %v", ids, want)
				}
			})
		}
	}
}

func TestWhereHas_ReplacesCollidingRelatedAlias(t *testing.T) {
	db := setupSaveDB(t)
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE self_relation_nodes (id INTEGER PRIMARY KEY, parent_id INTEGER);
		INSERT INTO self_relation_nodes VALUES (1, NULL), (2, 1)`); err != nil {
		t.Fatal(err)
	}
	q := New[selfRelationNode]().SetDB(db).Table("self_relation_nodes AS zorm_related")
	config := HasMany[selfRelationNode]{ForeignKey: "parent_id", Table: "self_relation_nodes AS zorm_related"}
	q.applyWhereHas([]reflect.Value{reflect.ValueOf(config)}, nil, "Children")
	rows, err := q.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		defer ClearOriginals(row)
	}
	if len(rows) != 1 || rows[0].ID != 1 {
		t.Fatalf("colliding alias returned wrong parents: %v", rows)
	}
}
