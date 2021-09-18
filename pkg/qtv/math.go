package qtv

import "time"

//
// Misc math utils.
//

// Bound val with min and max values.
func bound(min, val, max float64) float64 {
	if min >= max {
		return min
	}
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

// Bound val with min and max values.
func iBound(min, val, max int) int {
	if min >= max {
		return min
	}
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

// Bound val with min and max values.
func uBound(min, val, max uint) uint {
	if min >= max {
		return min
	}
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

// Bound val with min and max values.
func durationBound(min, val, max time.Duration) time.Duration {
	if min >= max {
		return min
	}
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

// Convert bool to byte.
func bool2byte(v bool) (b byte) {
	if v {
		return 1
	}
	return
}
