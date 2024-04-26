package lake

import (
	"strconv"
	"strings"
)

func getNumericPart(filename string, index int) int64 {
	parts := strings.Split(strings.ReplaceAll(filename, ".", "_"), "_")
	if len(parts) > 0 {
		num, err := strconv.Atoi(parts[index])
		if err == nil {
			return int64(num)
		}
	}
	return 0 // 如果解析失败，返回0（或者可以选择处理错误）
}
