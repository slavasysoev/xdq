package distance

import "testing"

func TestLevenshtein(t *testing.T) {
	tests := []struct {
		left, right string
		limit, want int
	}{
		{left: "orders.created", right: "orders.created", limit: 2, want: 0},
		{left: "orders.creatd", right: "orders.created", limit: 2, want: 1},
		{left: "orders-created", right: "orders.created", limit: 2, want: 1},
		{left: "ordres.created", right: "orders.created", limit: 2, want: 2},
		{left: "orders", right: "invoices", limit: 2, want: 3},
		{left: "a", right: "abcd", limit: 2, want: 3},
		{left: "", right: "ab", limit: 2, want: 2},
	}
	for _, test := range tests {
		got := Levenshtein(test.left, test.right, test.limit)
		if got != test.want {
			t.Errorf("Levenshtein(%q, %q, %d) = %d, want %d", test.left, test.right, test.limit, got, test.want)
		}
	}
}
