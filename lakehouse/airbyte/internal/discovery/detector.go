package discovery

import (
	"strings"

	"github.com/datalakehouse/airbyte-config-tool/internal/model"
)

// DetectSensitivity inspects column name and type to propose masking
func DetectSensitivity(columnName string, dataType string) (bool, model.MaskingType) {
	nameLower := strings.ToLower(columnName)

	// High-risk credentials -> Exclude / Null-out
	if strings.Contains(nameLower, "password") ||
		strings.Contains(nameLower, "secret") ||
		strings.Contains(nameLower, "token") ||
		strings.Contains(nameLower, "salt") ||
		strings.Contains(nameLower, "hash") ||
		strings.Contains(nameLower, "auth_key") {
		return true, model.MaskExclude
	}

	// Financial / Payment
	if strings.Contains(nameLower, "card") ||
		strings.Contains(nameLower, "credit") ||
		strings.Contains(nameLower, "cvv") ||
		strings.Contains(nameLower, "bank_acc") ||
		strings.Contains(nameLower, "pan") {
		return true, model.MaskCard
	}

	// Government ID / Tax / SSN
	if strings.Contains(nameLower, "ssn") ||
		strings.Contains(nameLower, "tax_id") ||
		strings.Contains(nameLower, "cccd") ||
		strings.Contains(nameLower, "cmnd") ||
		strings.Contains(nameLower, "identity") ||
		strings.Contains(nameLower, "passport") {
		return true, model.MaskMD5
	}

	// Email
	if strings.Contains(nameLower, "email") || strings.Contains(nameLower, "mail") {
		return true, model.MaskEmail
	}

	// Phone
	if strings.Contains(nameLower, "phone") ||
		strings.Contains(nameLower, "mobile") ||
		strings.Contains(nameLower, "tel") ||
		strings.Contains(nameLower, "sdt") {
		return true, model.MaskPhone
	}

	return false, model.MaskNone
}

// DetectCursorColumn checks if column is suitable for incremental cursor
func DetectCursorColumn(columnName string, dataType string) bool {
	nameLower := strings.ToLower(columnName)
	typeLower := strings.ToLower(dataType)

	if strings.Contains(typeLower, "time") || strings.Contains(typeLower, "date") {
		if strings.Contains(nameLower, "update") ||
			strings.Contains(nameLower, "modify") ||
			strings.Contains(nameLower, "edit") ||
			strings.Contains(nameLower, "create") {
			return true
		}
	}

	return false
}
