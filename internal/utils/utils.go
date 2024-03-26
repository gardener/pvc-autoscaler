package utils

import (
	"strconv"
	"strings"
)

// ParsePercentage parses a string value, which represents percentage, e.g. 10%.
func ParsePercentage(s string) (float64, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimRight(s, "%")
	val, err := strconv.ParseFloat(s, 64)

	return val, err
}
