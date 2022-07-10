package logging

import (
	"fmt"
	stdlog "log"
	"os"
	"strings"
)

type Level int

const (
	Trace Level = iota
	Debug
	Info
	Warn
	Error
)

const (
	DefaultLevel = Info
)

func ParseLevel(str string) Level {
	switch strings.ToUpper(str) {
	case "TRACE":
		return Trace
	case "DEBUG":
		return Debug
	case "INFO":
		return Info
	case "WARNING":
		return Warn
	case "ERROR":
		return Error
	}

	panic(fmt.Sprintf("log level %s not recognized", str))
}

func (l Level) String() string {
	switch l {
	case Trace:
		return "TRACE"
	case Debug:
		return "DEBUG"
	case Info:
		return "INFO"
	case Warn:
		return "WARNING"
	case Error:
		return "ERROR"
	}

	panic(fmt.Sprintf("unknown log level: %d", l))
}

type Logger interface {
	Tracef(format string, v ...interface{})
	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Errorf(format string, v ...interface{})

	Child(childPrefix string) Logger
}

func NewLogger(prefix interface{}, level Level) Logger {
	p := fmt.Sprintf("%v", prefix)
	return newStdImpl(p, level)
}

type stdImpl struct {
	log    *stdlog.Logger
	prefix string
	level  Level
}

func newStdImpl(prefix string, level Level) *stdImpl {
	return &stdImpl{
		log:    stdlog.New(os.Stdout, "", stdlog.Ltime|stdlog.Lmicroseconds),
		prefix: prefix,
		level:  level,
	}
}

func (s *stdImpl) Child(childPrefix string) Logger {
	prefix := fmt.Sprintf("%s/%s", s.prefix, childPrefix)
	return newStdImpl(prefix, s.level)
}

func (s *stdImpl) Tracef(format string, v ...interface{}) {
	s.print(Trace, format, v...)
}

func (s *stdImpl) Debugf(format string, v ...interface{}) {
	s.print(Debug, format, v...)
}

func (s *stdImpl) Infof(format string, v ...interface{}) {
	s.print(Info, format, v...)
}

func (s *stdImpl) Warnf(format string, v ...interface{}) {
	s.print(Warn, format, v...)
}

func (s *stdImpl) Errorf(format string, v ...interface{}) {
	s.print(Error, format, v...)
}

func (s *stdImpl) print(level Level, format string, v ...interface{}) {
	if s.level > level {
		return
	}

	prefixed := s.withPrefix(format)
	leveled := withLevelPrefix(level, prefixed)

	s.log.Printf(leveled, v...)
}

func withLevelPrefix(level Level, message string) string {
	return fmt.Sprintf("%s: %s", level.String(), message)
}

func (s *stdImpl) withPrefix(message string) string {
	if s.prefix == "" {
		return message
	}
	return fmt.Sprintf("[%s] %s", s.prefix, message)
}
