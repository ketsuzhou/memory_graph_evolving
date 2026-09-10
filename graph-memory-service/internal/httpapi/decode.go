package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// strictObject is a decoded request object with duplicate-key, trailing-value,
// and syntax checking already applied. Field-level type and rule checks happen
// per endpoint through the helpers below.
type strictObject map[string]json.RawMessage

type invalidRequest struct {
	jsonError bool
	fields    []fieldDetail
}

type fieldDetail struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

func (e *invalidRequest) Error() string {
	if e.jsonError {
		return "request body is not strictly valid JSON"
	}
	return "request violates the protocol request shape"
}

func decodeStrictObject(body []byte) (strictObject, *invalidRequest) {
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	if err := scanValue(decoder); err != nil {
		return nil, &invalidRequest{jsonError: true}
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, &invalidRequest{jsonError: true}
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(body, &object); err != nil {
		return nil, &invalidRequest{jsonError: true}
	}
	return object, nil
}

// scanValue walks one JSON value, rejecting duplicate object keys anywhere in
// the document.
func scanValue(dec *json.Decoder) error {
	token, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]bool)
		for dec.More() {
			keyToken, err := dec.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object key is not a string")
			}
			if seen[key] {
				return fmt.Errorf("duplicate object key %q", key)
			}
			seen[key] = true
			if err := scanValue(dec); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return err
		}
	case '[':
		for dec.More() {
			if err := scanValue(dec); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return err
		}
	}
	return nil
}

// fieldSet validates that the object contains exactly the allowed keys.
func (o strictObject) rejectUnknownFields(allowed map[string]bool) *invalidRequest {
	var fields []fieldDetail
	for key := range o {
		if !allowed[key] {
			fields = append(fields, fieldDetail{Field: key, Reason: "unknown field"})
		}
	}
	if fields == nil {
		return nil
	}
	return &invalidRequest{fields: fields}
}

func (o strictObject) requireString(key string, dst *string, errs *[]fieldDetail) {
	raw, ok := o[key]
	if !ok {
		*errs = append(*errs, fieldDetail{Field: key, Reason: "required field is missing"})
		return
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		markJSONError(errs)
	}
}

func (o strictObject) requireNullableString(key string, dst **string, errs *[]fieldDetail) {
	raw, ok := o[key]
	if !ok {
		*errs = append(*errs, fieldDetail{Field: key, Reason: "required field is missing"})
		return
	}
	if string(raw) == "null" {
		*dst = nil
		return
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		markJSONError(errs)
		return
	}
	*dst = &value
}

func (o strictObject) requireBool(key string, dst *bool, errs *[]fieldDetail) {
	raw, ok := o[key]
	if !ok {
		*errs = append(*errs, fieldDetail{Field: key, Reason: "required field is missing"})
		return
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		markJSONError(errs)
	}
}

func (o strictObject) requireInt(key string, dst *int, errs *[]fieldDetail) {
	raw, ok := o[key]
	if !ok {
		*errs = append(*errs, fieldDetail{Field: key, Reason: "required field is missing"})
		return
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		markJSONError(errs)
	}
}

func (o strictObject) requireInt64(key string, dst *int64, errs *[]fieldDetail) {
	raw, ok := o[key]
	if !ok {
		*errs = append(*errs, fieldDetail{Field: key, Reason: "required field is missing"})
		return
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		markJSONError(errs)
	}
}

func (o strictObject) requireStringArray(key string, dst *[]string, errs *[]fieldDetail) {
	raw, ok := o[key]
	if !ok {
		*errs = append(*errs, fieldDetail{Field: key, Reason: "required field is missing"})
		return
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		markJSONError(errs)
	}
}

// markJSONError records a type failure. The sentinel detail makes the whole
// request INVALID_JSON regardless of other field errors.
func markJSONError(errs *[]fieldDetail) {
	*errs = append(*errs, fieldDetail{Field: "", Reason: jsonErrorSentinel})
}

const jsonErrorSentinel = "\x00invalid-json"

func invalidRequestFrom(errs []fieldDetail) *invalidRequest {
	for _, detail := range errs {
		if detail.Reason == jsonErrorSentinel {
			return &invalidRequest{jsonError: true}
		}
	}
	cleaned := make([]fieldDetail, 0, len(errs))
	for _, detail := range errs {
		if detail.Reason == jsonErrorSentinel {
			continue
		}
		cleaned = append(cleaned, detail)
	}
	return &invalidRequest{fields: cleaned}
}

func addFieldError(errs *[]fieldDetail, field, reason string) {
	*errs = append(*errs, fieldDetail{Field: field, Reason: reason})
}
