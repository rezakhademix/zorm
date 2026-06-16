package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

// Post is the ent schema mirror of the shared bench posts table.
type Post struct {
	ent.Schema
}

func (Post) Fields() []ent.Field {
	return []ent.Field{
		field.String("title"),
		field.String("body"),
		field.Int64("views"),
		field.Float("rating"),
		field.Bool("published"),
		field.String("tags"),
		field.Bytes("cover").Optional(),
		field.Time("created_at").Default(time.Now),
	}
}

func (Post) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("user", User.Type).Ref("posts").Unique().Required(),
	}
}
