// Author: Liran Funaro <liran.funaro@ibm.com>

package utils

import (
	"log"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

type FatalLogger interface {
	Fatalf(template string, args ...interface{})
}

func Check(lg FatalLogger, err error) {
	if err != nil {
		lg.Fatalf("Failed with error: %s\n%v", err, zap.StackSkip("stack", 2).String)
	}
}

func CheckDefault(err error) {
	Check(log.Default(), err)
}

func GetFolderSize(path string) int64 {
	var size int64 = 0
	_ = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}
