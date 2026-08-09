package cron_test

import (
	"fmt"
	"time"

	"github.com/lcylpzls/jobx/cron"
)

// ExampleParse 演示 cron 表达式解析与下次触发计算。
func ExampleParse() {
	e, err := cron.Parse("0 9 * * 1")
	if err != nil {
		panic(err)
	}
	base := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC) // 周五。
	next, err := e.Next(base)
	if err != nil {
		panic(err)
	}
	fmt.Println(next.Format("2006-01-02 15:04"))
	// Output: 2026-08-10 09:00
}
