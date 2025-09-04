// tracer/validation/validator.go
package validation

import (
	"encoding/json"
)

func IsValidJSON(data []byte) bool {
	var js json.RawMessage
	return json.Unmarshal(data, &js) == nil
}
