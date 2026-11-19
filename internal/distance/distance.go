// Package distance measures how far apart two names are, to suggest what a mistyped name meant.
package distance

// Levenshtein returns the edit distance of the two strings, or limit+1 as soon as it is known to
// exceed limit. The early exit matters: every word of every configuration file is compared with
// every topic.
func Levenshtein(left, right string, limit int) int {
	if abs(len(left)-len(right)) > limit {
		return limit + 1
	}
	previous := make([]int, len(right)+1)
	current := make([]int, len(right)+1)
	for index := range previous {
		previous[index] = index
	}
	for leftIndex := 1; leftIndex <= len(left); leftIndex++ {
		current[0] = leftIndex
		rowMinimum := current[0]
		for rightIndex := 1; rightIndex <= len(right); rightIndex++ {
			substitution := previous[rightIndex-1]
			if left[leftIndex-1] != right[rightIndex-1] {
				substitution++
			}
			current[rightIndex] = min(previous[rightIndex]+1, current[rightIndex-1]+1, substitution)
			rowMinimum = min(rowMinimum, current[rightIndex])
		}
		if rowMinimum > limit {
			return limit + 1
		}
		previous, current = current, previous
	}

	return previous[len(right)]
}

func abs(value int) int {
	if value < 0 {
		return -value
	}
	return value
}
