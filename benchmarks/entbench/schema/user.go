package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

// User is the ent schema mirror of the shared bench users table.
type User struct {
	ent.Schema
}

func (User) Fields() []ent.Field {
	return []ent.Field{
		field.String("name"),
		field.String("email").Unique(),
		field.Int64("age"),
		field.Float("score"),
		field.Bool("is_active"),
		field.String("nickname").Optional().Nillable(),
		field.Bytes("avatar").Optional(),
		field.String("metadata"),
		field.Time("created_at").Default(time.Now),
	}
}

func (User) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("posts", Post.Type),
	}
}
