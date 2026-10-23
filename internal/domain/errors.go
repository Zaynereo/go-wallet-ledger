package domain

// Error is a client-facing failure with a stable code. The HTTP layer maps
// Status onto the response. Unknown internal failures are not Error values;
// handlers log those and return a generic 500.
type Error struct {
	Code    string
	Message string
	Fields  map[string]string
	Status  int
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func validation(message string, fields map[string]string) *Error {
	return &Error{Code: "validation_error", Message: message, Fields: fields, Status: 400}
}

func InsufficientFunds() *Error {
	return &Error{
		Code:    "insufficient_funds",
		Message: "account balance is too low for this debit",
		Status:  422,
	}
}

func CurrencyMismatch() *Error {
	return &Error{
		Code:    "currency_mismatch",
		Message: "currency does not match the account, or the accounts use different currencies",
		Status:  422,
	}
}

func SameAccount() *Error {
	return &Error{
		Code:    "same_account",
		Message: "the source and destination accounts must be different",
		Status:  422,
	}
}

func InvalidAccountKind(message string) *Error {
	return &Error{Code: "invalid_account_kind", Message: message, Status: 422}
}

func NotFound(message string) *Error {
	return &Error{Code: "not_found", Message: message, Status: 404}
}

func IdempotencyKeyRequired() *Error {
	return &Error{
		Code:    "idempotency_key_required",
		Message: "Idempotency-Key header is required",
		Status:  400,
	}
}

func IdempotencyConflict() *Error {
	return &Error{
		Code:    "idempotency_conflict",
		Message: "this Idempotency-Key was already used with a different request",
		Status:  409,
	}
}

func AccountExists() *Error {
	return &Error{
		Code:    "account_exists",
		Message: "account id already exists with a different currency or kind",
		Status:  409,
	}
}

func Overflow() *Error {
	return &Error{
		Code:    "overflow",
		Message: "balance would overflow the signed 64-bit range",
		Status:  422,
	}
}

func TemporarilyUnavailable() *Error {
	return &Error{
		Code:    "temporarily_unavailable",
		Message: "the operation could not be completed because of concurrent updates; retry with the same Idempotency-Key",
		Status:  503,
	}
}

func InvalidRequest(message string) *Error {
	return validation(message, nil)
}

func UnsupportedMediaType() *Error {
	return &Error{
		Code:    "unsupported_media_type",
		Message: "Content-Type must be application/json",
		Status:  415,
	}
}
