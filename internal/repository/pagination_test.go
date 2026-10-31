package repository

import (
	"testing"
)

func TestNormalizePagination(t *testing.T) {
	tests := []struct {
		name             string
		inPage, inSize   int
		outPage, outSize int
	}{
		{"defaults on zero", 0, 0, 1, 20},
		{"negative inputs", -5, -10, 1, 20},
		{"valid inputs within range", 3, 50, 3, 50},
		{"caps size at 100", 2, 250, 2, 100},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, s := NormalizePagination(tc.inPage, tc.inSize)
			if p != tc.outPage || s != tc.outSize {
				t.Errorf("NormalizePagination(%d, %d) = (%d, %d), expected (%d, %d)",
					tc.inPage, tc.inSize, p, s, tc.outPage, tc.outSize)
			}
		})
	}
}

func TestCalculateOffset(t *testing.T) {
	tests := []struct {
		page, size, expected int
	}{
		{1, 20, 0},
		{2, 20, 20},
		{2, 5, 5},
		{3, 10, 20},
	}

	for _, tc := range tests {
		got := CalculateOffset(tc.page, tc.size)
		if got != tc.expected {
			t.Errorf("CalculateOffset(%d, %d) = %d, expected %d", tc.page, tc.size, got, tc.expected)
		}
	}
}

func TestCalculateTotalPages(t *testing.T) {
	tests := []struct {
		total, size, expected int
	}{
		{0, 20, 0},
		{1, 20, 1},
		{20, 20, 1},
		{21, 20, 2},
		{45, 10, 5},
		{100, 20, 5},
	}

	for _, tc := range tests {
		got := CalculateTotalPages(tc.total, tc.size)
		if got != tc.expected {
			t.Errorf("CalculateTotalPages(%d, %d) = %d, expected %d", tc.total, tc.size, got, tc.expected)
		}
	}
}
