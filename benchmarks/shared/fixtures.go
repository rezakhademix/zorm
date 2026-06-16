package shared

import (
	"fmt"
	"math/rand"
	"time"
)

// SeedSize is the number of users seeded by SeedRaw. Each user gets PostsPerUser
// posts, so the posts table holds SeedSize*PostsPerUser rows.
const (
	SeedSize     = 1000
	PostsPerUser = 5
)

// FixedTime is the timestamp every fixture row carries so that comparisons
// across ORMs aren't muddied by clock drift between b.N iterations.
var FixedTime = time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

// UserPayload is the portable shape every ORM bench file uses to build its own
// strongly-typed insert call. The fields map 1:1 to the users table.
type UserPayload struct {
	Name      string
	Email     string
	Age       int64
	Score     float64
	IsActive  bool
	Nickname  *string
	Avatar    []byte
	Metadata  string
	CreatedAt time.Time
}

// PostPayload mirrors UserPayload for the posts table.
type PostPayload struct {
	UserID    int64
	Title     string
	Body      string
	Views     int64
	Rating    float64
	Published bool
	Tags      string
	Cover     []byte
	CreatedAt time.Time
}

// NewRNG returns a deterministic RNG so every ORM sees identical fixture content.
func NewRNG() *rand.Rand { return rand.New(rand.NewSource(42)) }

// MakeUser builds a deterministic UserPayload from i so callers can produce the
// same value across runs without sharing state.
func MakeUser(i int) UserPayload {
	nick := fmt.Sprintf("nick_%d", i)
	var nickname *string
	if i%3 != 0 {
		nickname = &nick
	}
	return UserPayload{
		Name:      fmt.Sprintf("user_%d", i),
		Email:     fmt.Sprintf("user_%d@example.com", i),
		Age:       int64(20 + i%50),
		Score:     float64(i%1000) / 7.0,
		IsActive:  i%2 == 0,
		Nickname:  nickname,
		Avatar:    []byte{byte(i), byte(i >> 8), 0xAA, 0xBB},
		Metadata:  fmt.Sprintf(`{"i":%d,"tier":"gold"}`, i),
		CreatedAt: FixedTime,
	}
}

// MakePost builds a deterministic PostPayload owned by userID.
func MakePost(userID int64, j int) PostPayload {
	return PostPayload{
		UserID:    userID,
		Title:     fmt.Sprintf("title_%d_%d", userID, j),
		Body:      fmt.Sprintf("body_%d_%d", userID, j),
		Views:     int64(j * 100),
		Rating:    float64(j) * 1.5,
		Published: j%2 == 0,
		Tags:      `["go","bench"]`,
		Cover:     []byte{0xC0, 0xFF, 0xEE, byte(j)},
		CreatedAt: FixedTime,
	}
}
