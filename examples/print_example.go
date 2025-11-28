package main

import (
	"fmt"

	"github.com/rezakhademix/zorm"
)

type User struct {
	ID     int
	Name   string
	Email  string
	Age    int
	Status string
}

func (User) TableName() string {
	return "users"
}

func main() {
	// Initialize model
	m := zorm.New[User]()

	// Example 1: Simple query
	sql, args := m.Where("status", "active").Print()
	fmt.Println("Query 1:")
	fmt.Println("SQL:", sql)
	fmt.Println("Args:", args)
	fmt.Println()

	// Example 2: Complex query
	m2 := zorm.New[User]()
	sql, args = m2.
		Select("id", "name", "email").
		Where("age", 25).
		Where("status", "active").
		OrderBy("created_at", "DESC").
		Limit(10).
		Offset(5).
		Print()
	fmt.Println("Query 2:")
	fmt.Println("SQL:", sql)
	fmt.Println("Args:", args)
	fmt.Println()

	// Example 3: Full-text search
	m3 := zorm.New[User]()
	sql, args = m3.WhereFullText("bio", "golang developer").Limit(20).ToSQL()
	fmt.Println("Query 3:")
	fmt.Println("SQL:", sql)
	fmt.Println("Args:", args)
	fmt.Println()

	// Example 4: Raw query
	m4 := zorm.New[User]()
	sql, args = m4.Raw("SELECT * FROM users WHERE email = ? AND age > ?", "test@example.com", 18).Print()
	fmt.Println("Query 4:")
	fmt.Println("SQL:", sql)
	fmt.Println("Args:", args)
	fmt.Println()

	// Example 5: Multiple conditions
	m5 := zorm.New[User]()
	sql, args = m5.
		Where("status", "active").
		OrWhere("status", "pending").
		Where("age >=", 18).
		Print()
	fmt.Println("Query 5:")
	fmt.Println("SQL:", sql)
	fmt.Println("Args:", args)
}
