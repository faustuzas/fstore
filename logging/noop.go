package logging

type noopLogger int

var Noop Logger = noopLogger(0)

func (n noopLogger) Tracef(format string, v ...interface{}) {
	// noop...
}

func (n noopLogger) Debugf(format string, v ...interface{}) {
	// noop...
}

func (n noopLogger) Infof(format string, v ...interface{}) {
	// noop...
}

func (n noopLogger) Warnf(format string, v ...interface{}) {
	// noop...
}

func (n noopLogger) Errorf(format string, v ...interface{}) {
	// noop...
}

func (n noopLogger) Child(childPrefix string) Logger {
	return n
}
