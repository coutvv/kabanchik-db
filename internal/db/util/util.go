package util

import (
	"fmt"
	"time"
)

func Timer(name string) func() {
	start := time.Now()
	return func() {
		fmt.Printf("%s took %v\n", name, time.Since(start))
	}
}

func WaitForIt(seconds int) {
	time.Sleep(time.Duration(seconds * 1_000_000_000))
}
