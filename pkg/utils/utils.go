package utils

type FatalLogger interface {
	Fatalf(template string, args ...interface{})
}

func Check(lg FatalLogger, err error) {
	if err != nil {
		lg.Fatalf("Failed with error: %s", err)
	}
}
