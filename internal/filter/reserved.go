package filter

import "strings"

var reservedParams = map[string]bool{
	"page":        true,
	"pagesize":    true,
	"per_page":    true,
	"limit":       true,
	"offset":      true,
	"sort":        true,
	"order":       true,
	"order_by":    true,
	"order_dir":   true,
	"instance_id": true,
	"org_id":      true,
}

func isReservedParam(param string) bool {
	return reservedParams[strings.ToLower(param)]
}
