// Code generated by "stringer"; DO NOT EDIT.

package kvcoord

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[txnPending-0]
	_ = x[txnRetryableError-1]
	_ = x[txnError-2]
	_ = x[txnPrepared-3]
	_ = x[txnFinalized-4]
}

func (i txnState) String() string {
	switch i {
	case txnPending:
		return "txnPending"
	case txnRetryableError:
		return "txnRetryableError"
	case txnError:
		return "txnError"
	case txnPrepared:
		return "txnPrepared"
	case txnFinalized:
		return "txnFinalized"
	default:
		return "txnState(" + strconv.FormatInt(int64(i), 10) + ")"
	}
}
