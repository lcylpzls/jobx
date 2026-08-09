package cron

import (
	"testing"
	"time"

	"github.com/lcylpzls/errx"
)

// TestParseErrors 覆盖解析错误分支。
func TestParseErrors(t *testing.T) {
	cases := []string{
		"",
		"* * * *",
		"* * * * * * *",
		"61 * * * * *",
		"* 60 * * *",
		"* * 24 * * *",
		"* * 32 * *",
		"* * * 0 *",
		"* * * * 7",
		"* * * 32 *",
		"* * * * 13",
		"a * * * *",
		"5-1 * * * *",
		"*/0 * * * *",
		"1-2-3 * * * *",
		"1/2/3 * * * *",
	}
	for _, expr := range cases {
		if _, err := Parse(expr); err == nil || !errx.Is(err, CodeCronInvalid) {
			t.Fatalf("表达式 %q 应报错，实际：%v", expr, err)
		}
	}
}

// TestParseValid 覆盖合法解析与规范化。
func TestParseValid(t *testing.T) {
	for _, expr := range []string{
		"* * * * *",
		"0 3 * * *",
		"0 0 1 * *",
		"0 0 * * 1",
		"*/5 * * * *",
		"1,15,30 * * * *",
		"10-20 * * * *",
		"0 0 12 * * *",
		"0 0 1 * 1",
		"30 8 ? * *",
	} {
		e, err := Parse(expr)
		if err != nil {
			t.Fatalf("表达式 %q 应合法：%v", expr, err)
		}
		if e.String() != expr {
			t.Fatalf("原文不符：%q != %q", e.String(), expr)
		}
	}
}

// TestNext 覆盖典型触发场景。
func TestNext(t *testing.T) {
	base := time.Date(2026, 8, 9, 10, 15, 30, 0, time.UTC)
	cases := []struct {
		expr string
		at   time.Time
		want time.Time
	}{
		{"* * * * *", base, time.Date(2026, 8, 9, 10, 16, 0, 0, time.UTC)},
		{"0 3 * * *", base, time.Date(2026, 8, 10, 3, 0, 0, 0, time.UTC)},
		{"0 0 1 * *", base, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)},
		{"0 0 * * 1", base, time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)},
		{"*/15 * * * *", base, time.Date(2026, 8, 9, 10, 30, 0, 0, time.UTC)},
		{"0 0 12 * * *", base, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)},
		{"30 10 * * *", base, time.Date(2026, 8, 9, 10, 30, 0, 0, time.UTC)},
		{"0 0 29 2 *", time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC),
			time.Date(2028, 2, 29, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		e, err := Parse(tc.expr)
		if err != nil {
			t.Fatal(err)
		}
		got, err := e.Next(tc.at)
		if err != nil {
			t.Fatalf("%q Next 失败：%v", tc.expr, err)
		}
		if !got.Equal(tc.want) {
			t.Fatalf("%q 在 %v 应得 %v，实际 %v", tc.expr, tc.at, tc.want, got)
		}
	}
}

// TestNextStrict 覆盖“严格晚于”语义。
func TestNextStrict(t *testing.T) {
	e, err := Parse("0 0 12 * * *")
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	got, err := e.Next(at)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(at.AddDate(0, 0, 1)) {
		t.Fatalf("严格晚于应返回次日：%v", got)
	}
}

// TestNextN 覆盖批量触发计算。
func TestNextN(t *testing.T) {
	e, err := Parse("*/30 * * * * *")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	got, err := e.NextN(base, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Time{
		base.Add(30 * time.Second),
		base.Add(60 * time.Second),
		base.Add(90 * time.Second),
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("第 %d 次触发不符：%v != %v", i, got[i], want[i])
		}
	}
	if _, err := e.NextN(base, 0); err == nil || !errx.Is(err, CodeCronInvalid) {
		t.Fatalf("零次数应报错，实际：%v", err)
	}
}

// TestNextNoSolution 覆盖无解表达式。
func TestNextNoSolution(t *testing.T) {
	e, err := Parse("0 0 30 2 *")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	if _, err := e.Next(base); err == nil || !errx.Is(err, CodeCronInvalid) {
		t.Fatalf("2 月 30 日应无解，实际：%v", err)
	}
	if _, err := e.NextN(base, 3); err == nil || !errx.Is(err, CodeCronInvalid) {
		t.Fatalf("NextN 应传播无解错误，实际：%v", err)
	}
}

// TestLocation 覆盖时区保留。
func TestLocation(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	e, err := Parse("0 3 * * *")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 9, 10, 0, 0, 0, loc)
	got, err := e.Next(base)
	if err != nil {
		t.Fatal(err)
	}
	if got.Location() != loc {
		t.Fatalf("应保留时区：%v", got.Location())
	}
	if got.Hour() != 3 {
		t.Fatalf("应为当地 3 点：%v", got)
	}
}

// TestDayUnion 覆盖日与周取并集。
func TestDayUnion(t *testing.T) {
	e, err := Parse("0 0 1 * 1")
	if err != nil {
		t.Fatal(err)
	}
	// 2026-08-01 是周六；2026-08-03 是周一。应从周六的次日开始找周一（或下月1号）。
	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	got, err := e.Next(base)
	if err != nil {
		t.Fatal(err)
	}
	if got.Day() != 3 {
		t.Fatalf("并集应命中 8 月 3 日（周一）：%v", got)
	}
}

// TestNextMonthEnd 覆盖每月 31 号跳过小月。
func TestNextMonthEnd(t *testing.T) {
	e, err := Parse("0 0 31 * *")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC)
	got, err := e.Next(base)
	if err != nil {
		t.Fatal(err)
	}
	if got.Month() != time.May || got.Day() != 31 {
		t.Fatalf("应跳过无 31 号的月份：%v", got)
	}
}

// TestNextDST 覆盖夏令时切换日的触发计算（不 panic、返回合法时间）。
func TestNextDST(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	e, err := Parse("0 2 * * *")
	if err != nil {
		t.Fatal(err)
	}
	// 2026-03-08 02:00 在纽约不存在（跳变到 03:00）。
	base := time.Date(2026, 3, 8, 0, 0, 0, 0, loc)
	got, err := e.Next(base)
	if err != nil {
		t.Fatal(err)
	}
	if got.Location() != loc || got.Hour() < 2 || got.Hour() > 3 {
		t.Fatalf("DST 切换日结果异常：%v", got)
	}
}
