package logger

type Logi interface {
	Debugf(format string, m ...interface{})
	Infof(format string, m ...interface{})
	Warnf(format string, m ...interface{})
	Errorf(format string, m ...interface{})
}
