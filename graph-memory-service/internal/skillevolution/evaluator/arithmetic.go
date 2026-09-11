package evaluator

import "math/bits"

// Checked integer arithmetic (Contract §10.3/GMS §5.5: sufficiently wide
// or checked integers; overflow fails closed, never saturates, never
// truncates and never touches a float).

type overflowError struct{}

func (overflowError) Error() string { return "evaluator: checked integer overflow" }

var errArithmeticOverflow = overflowError{}

// checkedAdd adds two int64s fail-closed on overflow.
func checkedAdd(a, b int64) (int64, error) {
	c := a + b
	if (b > 0 && c < a) || (b < 0 && c > a) {
		return 0, errArithmeticOverflow
	}
	return c, nil
}

// checkedMul multiplies two int64s fail-closed on overflow. The domain of
// every call site is non-negative counts and >= 1 denominators; a negative
// operand is outside the domain and reports overflow rather than risking a
// wrong sign-extension result (fail closed, never silently truncate).
func checkedMul(a, b int64) (int64, error) {
	if a < 0 || b < 0 {
		return 0, errArithmeticOverflow
	}
	if a == 0 || b == 0 {
		return 0, nil
	}
	hi, lo := bits.Mul64(uint64(a), uint64(b))
	// For int64 operands the 128-bit product fits int64 iff the upper 64
	// bits are a correct sign extension of the lower 64 bits.
	if hi != uint64(int64(lo))>>63 {
		return 0, errArithmeticOverflow
	}
	return int64(lo), nil
}

// crossAtLeast reports a*d >= b*c for non-negative int64 operands using
// checked products only (a ratio inequality proven without dividing; d
// plays the frozen-ratio denominator and must be >= 1).
func crossAtLeast(a, b, c, d int64) (bool, error) {
	if a < 0 || b < 0 || c < 0 || d < 1 {
		return false, errArithmeticOverflow
	}
	left, err := checkedMul(a, d)
	if err != nil {
		return false, err
	}
	right, err := checkedMul(b, c)
	if err != nil {
		return false, err
	}
	return left >= right, nil
}

// compareRatios compares the non-negative rationals aNum/aDen and
// bNum/bDen with integer cross multiplication only:
//
//	aNum * bDen  vs  bNum * aDen
//
// Returns -1, 0 or +1. Any negative operand, non-positive denominator or
// overflowing cross product fails closed (the comparison cannot be proven
// correct — the caller MUST treat that as UTILITY_ARITHMETIC_OVERFLOW,
// never as an ordering).
func compareRatios(aNum, aDen, bNum, bDen int64) (int, error) {
	if aNum < 0 || bNum < 0 || aDen <= 0 || bDen <= 0 {
		return 0, errArithmeticOverflow
	}
	left, err := checkedMul(aNum, bDen)
	if err != nil {
		return 0, err
	}
	right, err := checkedMul(bNum, aDen)
	if err != nil {
		return 0, err
	}
	switch {
	case left < right:
		return -1, nil
	case left > right:
		return 1, nil
	default:
		return 0, nil
	}
}
