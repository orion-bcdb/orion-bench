package utils

import (
	"log"

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
