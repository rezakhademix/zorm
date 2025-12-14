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
