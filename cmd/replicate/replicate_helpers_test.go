package main_test

import "os"

func makeDir(p string) error { return os.MkdirAll(p, 0o755) }
