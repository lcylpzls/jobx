package cron

import (
	"testing"
	"time"
)

// FuzzCron 模糊测试表达式解析与触发计算，确保任意输入不 panic。
func FuzzCron(f *testing.F) {
	f.Add("* * * * *")
	f.Add("0 0 12 * * *")
	f.Add("*/5 * * * *")
	f.Add("")
	f.Add("bad")
	f.Fuzz(func(t *testing.T, expr string) {
		if len(expr) > 512 {
			t.Skip("输入过大")
		}
		e, err := Parse(expr)
		if err != nil {
			return
		}
		base := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
		_, _ = e.Next(base)
		_, _ = e.NextN(base, 2)
	})
}
