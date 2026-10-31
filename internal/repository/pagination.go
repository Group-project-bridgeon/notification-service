package repository

// NormalizePagination ensures page is at least 1 and pageSize is between 1 and 100 (default 20).
func NormalizePagination(page, pageSize int) (int, int) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	} else if pageSize > 100 {
		pageSize = 100
	}
	return page, pageSize
}

// CalculateOffset returns the SQL OFFSET for a given 1-based page and page size.
func CalculateOffset(page, pageSize int) int {
	p, ps := NormalizePagination(page, pageSize)
	return (p - 1) * ps
}

// CalculateTotalPages calculates total pages given total record count and page size.
func CalculateTotalPages(totalCount, pageSize int) int {
	if totalCount <= 0 || pageSize <= 0 {
		return 0
	}
	return (totalCount + pageSize - 1) / pageSize
}
