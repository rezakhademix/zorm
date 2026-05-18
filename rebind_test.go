package zorm

import "testing"

func TestRebind(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Simple",
			input:    "SELECT * FROM users WHERE id = ?",
			expected: "SELECT * FROM users WHERE id = $1",
		},
		{
			name:     "Multiple",
			input:    "SELECT * FROM users WHERE name = ? AND age > ?",
			expected: "SELECT * FROM users WHERE name = $1 AND age > $2",
		},
		{
			name:     "Inside Quotes",
			input:    "SELECT * FROM users WHERE name = 'Question?' AND age = ?",
			expected: "SELECT * FROM users WHERE name = 'Question?' AND age = $1",
		},
		{
			name:     "Multiple Quotes",
			input:    "INSERT INTO table VALUES (?, 'Value?', ?, 'Another?')",
			expected: "INSERT INTO table VALUES ($1, 'Value?', $2, 'Another?')",
		},
		{
			name:     "Empty",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rebind(tt.input)
			if got != tt.expected {
				t.Errorf("rebind() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func BenchmarkRebind(b *testing.B) {
	query := "SELECT * FROM users WHERE name = ? AND age > ? AND status = ? AND created_at < ?"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = rebind(query)
	}
}

// TestRebind_EscapedSingleQuotes ensures `”` inside a single-quoted literal
// does not throw off the placeholder counter for ? appearing after the literal.
func TestRebind_EscapedSingleQuotes(t *testing.T) {
	input := "SELECT * FROM t WHERE label = 'It''s ?' AND id = ?"
	want := "SELECT * FROM t WHERE label = 'It''s ?' AND id = $1"
	if got := rebind(input); got != want {
		t.Errorf("rebind regressed on escaped-quote input\n got:  %s\n want: %s", got, want)
	}
}

// TestRebind_PreservesDollarQuotedStrings verifies that `?` inside a
// PostgreSQL dollar-quoted literal ($$ ... $$ or $tag$ ... $tag$) is left
// untouched — it is part of the string, not a bind placeholder.
func TestRebind_PreservesDollarQuotedStrings(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "anonymous tag",
			input: "SELECT $$keep this ? literal$$, id FROM t WHERE id = ?",
			want:  "SELECT $$keep this ? literal$$, id FROM t WHERE id = $1",
		},
		{
			name:  "named tag",
			input: "SELECT $body$ contains ? and ?? $body$ FROM t WHERE id = ?",
			want:  "SELECT $body$ contains ? and ?? $body$ FROM t WHERE id = $1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rebind(tc.input); got != tc.want {
				t.Errorf("\n got:  %s\n want: %s", got, tc.want)
			}
		})
	}
}

// TestRebind_PreservesJSONOperators verifies that:
//   - `??` is an escape for a literal `?` (intended for the JSON key-exists operator),
//   - `?|` and `?&` (JSON any/all-key operators) are not split,
//   - other bare `?` continue to be rewritten as `$N` placeholders.
func TestRebind_PreservesJSONOperators(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "?? escape",
			input: "SELECT * FROM t WHERE data ?? 'k' AND id = ?",
			want:  "SELECT * FROM t WHERE data ? 'k' AND id = $1",
		},
		{
			name:  "?| operator preserved",
			input: "SELECT * FROM t WHERE tags ?| array['a','b'] AND id = ?",
			want:  "SELECT * FROM t WHERE tags ?| array['a','b'] AND id = $1",
		},
		{
			name:  "?& operator preserved",
			input: "SELECT * FROM t WHERE tags ?& array['a','b'] AND id = ?",
			want:  "SELECT * FROM t WHERE tags ?& array['a','b'] AND id = $1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rebind(tc.input); got != tc.want {
				t.Errorf("\n got:  %s\n want: %s", got, tc.want)
			}
		})
	}
}

// TestRebind_PreservesComments verifies that `?` inside SQL comments
// (line `-- …` and block `/* … */`, including nested blocks) is not rewritten.
func TestRebind_PreservesComments(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "line comment",
			input: "SELECT * FROM t -- this ? is a comment\nWHERE id = ?",
			want:  "SELECT * FROM t -- this ? is a comment\nWHERE id = $1",
		},
		{
			name:  "block comment",
			input: "SELECT /* keep ? out */ id FROM t WHERE id = ?",
			want:  "SELECT /* keep ? out */ id FROM t WHERE id = $1",
		},
		{
			name:  "nested block comment",
			input: "SELECT /* a /* nested ? */ still here */ id FROM t WHERE id = ?",
			want:  "SELECT /* a /* nested ? */ still here */ id FROM t WHERE id = $1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rebind(tc.input); got != tc.want {
				t.Errorf("\n got:  %s\n want: %s", got, tc.want)
			}
		})
	}
}
