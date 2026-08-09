// Package cron 提供自研 cron 表达式解析与下次触发计算。
package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lcylpzls/errx"
)

// CodeCronInvalid cron 表达式非法（与 jobx 根包错误码同值）。
const CodeCronInvalid errx.Code = "jobx_cron_invalid"

// maxScanYears 无解表达式的扫描上限。
const maxScanYears = 5

// field 单个字段的取值集合。
type field struct {
	set [60]bool
	all bool
}

// Expr 解析后的 cron 表达式（5 或 6 字段）。
type Expr struct {
	raw   string
	sec   field
	min   field
	hour  field
	dom   field
	month field
	dow   field
}

// Parse 解析 5/6 字段 cron 表达式：
// 6 字段：秒 分 时 日 月 周；5 字段等价前导秒为 0。
func Parse(expr string) (*Expr, error) {
	parts := strings.Fields(expr)
	if len(parts) != 5 && len(parts) != 6 {
		return nil, parseError("字段数量必须为 5 或 6")
	}
	e := &Expr{raw: expr}
	idx := 0
	if len(parts) == 6 {
		f, err := parseField(parts[0], 0, 59, "秒")
		if err != nil {
			return nil, err
		}
		e.sec = f
		idx = 1
	} else {
		e.sec.set[0] = true
		e.sec.all = false
	}
	if f, err := parseField(parts[idx], 0, 59, "分"); err != nil {
		return nil, err
	} else {
		e.min = f
	}
	if f, err := parseField(parts[idx+1], 0, 23, "时"); err != nil {
		return nil, err
	} else {
		e.hour = f
	}
	if f, err := parseField(parts[idx+2], 1, 31, "日"); err != nil {
		return nil, err
	} else {
		e.dom = f
	}
	if f, err := parseField(parts[idx+3], 1, 12, "月"); err != nil {
		return nil, err
	} else {
		e.month = f
	}
	if f, err := parseField(parts[idx+4], 0, 6, "周"); err != nil {
		return nil, err
	} else {
		e.dow = f
	}
	return e, nil
}

// Next 返回严格晚于 after 的下一个触发时刻；无解（5 年内）返回错误。
func (e *Expr) Next(after time.Time) (time.Time, error) {
	t := after.Add(time.Second).Truncate(time.Second)
	limit := t.AddDate(maxScanYears, 0, 0)
	for t.Before(limit) {
		// 秒。
		if !e.sec.set[t.Second()] {
			if n := nextInSet(e.sec, t.Second()); n >= 0 {
				t = t.Add(time.Duration(n-t.Second()) * time.Second)
				continue
			}
			t = t.Truncate(time.Minute).Add(time.Minute)
			continue
		}
		// 分。
		if !e.min.set[t.Minute()] {
			if n := nextInSet(e.min, t.Minute()); n >= 0 {
				t = t.Truncate(time.Minute).Add(time.Duration(n-t.Minute()) * time.Minute)
				continue
			}
			t = t.Truncate(time.Hour).Add(time.Hour)
			continue
		}
		// 时。
		if !e.hour.set[t.Hour()] {
			advanced := false
			for h := t.Hour() + 1; h < 24; h++ {
				if !e.hour.set[h] {
					continue
				}
				candidate := time.Date(t.Year(), t.Month(), t.Day(), h, 0, 0, 0, t.Location())
				if candidate.Hour() == h { // 跳过 DST gap 中不存在的墙钟小时。
					t = candidate
					advanced = true
					break
				}
			}
			if advanced {
				continue
			}
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).AddDate(0, 0, 1)
			continue
		}
		// 日与周（双方均受限时取并集）。
		domOK := e.dom.set[t.Day()]
		dowOK := e.dow.set[int(t.Weekday())]
		dayOK := dayMatch(e.dom.all, e.dow.all, domOK, dowOK)
		if !dayOK {
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).AddDate(0, 0, 1)
			continue
		}
		// 月。
		if !e.month.set[int(t.Month())] {
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
			continue
		}
		return t, nil
	}
	return time.Time{}, parseError("表达式的下一个触发时刻超出扫描上限（5 年）")
}

// NextN 返回严格晚于 after 的 n 个触发时刻。
func (e *Expr) NextN(after time.Time, n int) ([]time.Time, error) {
	if n <= 0 {
		return nil, parseError("触发次数必须为正")
	}
	out := make([]time.Time, 0, n)
	next := after
	for i := 0; i < n; i++ {
		t, err := e.Next(next)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
		next = t
	}
	return out, nil
}

// String 返回原始表达式文本。
func (e *Expr) String() string {
	return e.raw
}

// parseField 解析单个字段：数字、*、*/n、a-b、a,b,c、?（等价 *）。
func parseField(s string, min, max int, name string) (field, error) {
	var f field
	all := false
	for _, part := range strings.Split(s, ",") {
		if part == "?" {
			part = "*"
		}
		step := 1
		base := part
		if strings.Contains(part, "/") {
			seg := strings.SplitN(part, "/", 2)
			base = seg[0]
			n, err := strconv.Atoi(seg[1])
			if err != nil || n <= 0 {
				return f, parseError(fmt.Sprintf("%s 字段步进值非法：%q", name, seg[1]))
			}
			step = n
		}
		var lo, hi int
		switch {
		case base == "*":
			lo, hi = min, max
			all = true
		case strings.Contains(base, "-"):
			seg := strings.SplitN(base, "-", 2)
			a, err1 := strconv.Atoi(seg[0])
			b, err2 := strconv.Atoi(seg[1])
			if err1 != nil || err2 != nil || a < min || b > max || a > b {
				return f, parseError(fmt.Sprintf("%s 字段范围非法：%q", name, base))
			}
			lo, hi = a, b
		default:
			n, err := strconv.Atoi(base)
			if err != nil || n < min || n > max {
				return f, parseError(fmt.Sprintf("%s 字段取值非法：%q", name, base))
			}
			lo, hi = n, n
		}
		for i := lo; i <= hi; i += step {
			f.set[i] = true
		}
	}
	f.all = all
	return f, nil
}

// nextInSet 返回集合中 >= v 的最小值；不存在返回 -1。
func nextInSet(f field, v int) int {
	for i := v; i < 60; i++ {
		if f.set[i] {
			return i
		}
	}
	return -1
}

// dayMatch 日与周字段的组合匹配（双方均受限时取并集）。
func dayMatch(domAll, dowAll, domOK, dowOK bool) bool {
	switch {
	case domAll && dowAll:
		return true
	case domAll:
		return dowOK
	case dowAll:
		return domOK
	default:
		return domOK || dowOK
	}
}

// parseError 构造 cron 表达式错误（含字段定位）。
func parseError(msg string) error {
	return errx.NewCode(CodeCronInvalid, msg)
}
