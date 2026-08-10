package main

import (
	"testing"

	"github.com/lcylpzls/testx"
)

// TestRun 验证基础示例可完整执行。
func TestRun(t *testing.T) {
	testx.RequireNoError(t, run())
}
