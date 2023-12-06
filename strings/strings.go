package strings

import (
	"strings"
)

// Combine 组合字符串.
func Combine(a ...string) string {
	var build strings.Builder
	for i := 0; i < len(a); i++ {
		build.WriteString(a[i])
	}
	return build.String()
}
