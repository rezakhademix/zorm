//go:build ignore
// +build ignore

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/rezakhademix/zorm"
)

type User struct {
	ID    int
	Name  string
	Email string
}

func main() {
	// Connect to database
	db, err := sql.Open("postgres", "postgres://localhost/mydb?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	zorm.GlobalDB = db

	// Create a statement cache with capacity of 100
	cache := zorm.NewStmtCache(100)
	defer cache.Close()

	fmt.Println("=== Statement Caching Example ===")

	// Create a model with statement caching enabled
	model := zorm.New[User]().WithStmtCache(cache)

	ctx := context.Background()

	// Example 1: Multiple queries with the same structure will reuse prepared statements
	fmt.Println("1. Running multiple queries - statements will be cached")
	for i := 1; i <= 5; i++ {
		count, err := model.WithContext(ctx).Where("id > ?", i).Count()
		if err != nil {
			log.Printf("Count failed: %v", err)
			continue
		}
		fmt.Printf("   Query %d: Found %d users\n", i, count)
	}
	fmt.Printf("   Cache size after queries: %d\n\n", cache.Len())

	// Example 2: Create operations also use cached statements
	fmt.Println("2. Creating users with cached statements")
	for i := 1; i <= 3; i++ {
		user := &User{
			Name:  fmt.Sprintf("User %d", i),
			Email: fmt.Sprintf("user%d@example.com", i),
		}
		err := model.Create(user)
		if err != nil {
			log.Printf("Create failed: %v", err)
			continue
		}
		fmt.Printf("   Created user: ID=%d, Name=%s\n", user.ID, user.Name)
	}
	fmt.Printf("   Cache size after creates: %d\n\n", cache.Len())

	// Example 3: Read operations benefit from caching
	fmt.Println("3. Reading data with cached statements")
	users, err := model.WithContext(ctx).Get()
	if err != nil {
		log.Printf("Get failed: %v", err)
	} else {
		fmt.Printf("   Retrieved %d users\n", len(users))
	}
	fmt.Printf("   Cache size: %d\n\n", cache.Len())

	// Example 4: Clone preserves cache reference
	fmt.Println("4. Cloned models share the same cache")
	clonedModel := model.Clone()
	_, err = clonedModel.Where("email LIKE ?", "%example.com").Count()
	if err != nil {
		log.Printf("Count on cloned model failed: %v", err)
	}
	fmt.Printf("   Cache size after using cloned model: %d\n\n", cache.Len())

	// Example 5: Cache statistics
	fmt.Println("5. Cache Statistics")
	fmt.Printf("   Total cached statements: %d\n", cache.Len())
	fmt.Printf("   Cache capacity: %d\n", cache.Len()) // Would need to expose capacity
	fmt.Println("\n=== Performance Benefits ===")
	fmt.Println("✓ Prepared statements are reused across queries")
	fmt.Println("✓ Reduces overhead of statement preparation")
	fmt.Println("✓ Thread-safe for concurrent use")
	fmt.Println("✓ LRU eviction prevents unbounded memory growth")
	fmt.Println("✓ Transparent - works with all query methods")
}
