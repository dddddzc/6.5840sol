package raft

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// AppendEntries需要的Min函数
func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
