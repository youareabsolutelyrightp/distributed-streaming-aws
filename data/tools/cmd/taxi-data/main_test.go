package main

import "testing"

func TestCanonicalFixedDecimal(t *testing.T) {
	tests := map[string]string{
		"0":      "0.00",
		"0002.5": "2.50",
		"-1":     "-1.00",
		"-0.00":  "0.00",
		"4.500":  "4.50",
	}
	for input, expected := range tests {
		actual, err := canonicalFixedDecimal(input, 2)
		if err != nil {
			t.Fatalf("canonicalFixedDecimal(%q): %v", input, err)
		}
		if actual != expected {
			t.Fatalf("canonicalFixedDecimal(%q)=%q, want %q", input, actual, expected)
		}
	}
	if _, err := canonicalFixedDecimal("1.234", 2); err == nil {
		t.Fatal("expected non-zero third decimal place to fail")
	}
}

func TestSortKeyComparisonUsesBothSourceRows(t *testing.T) {
	left := sortKey{dropoff: "2013-01-01T00:00:00Z", month: 1, tripRow: 5, fareRow: 7}
	right := sortKey{dropoff: "2013-01-01T00:00:00Z", month: 1, tripRow: 5, fareRow: 8}
	if compareKeys(left, right) >= 0 {
		t.Fatal("fare source row must break the final tie")
	}
}
