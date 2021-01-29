package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"sync"
)

var loggerMutex sync.RWMutex // guards access to global logger state

// loggers is the set of loggers in the system
var loggers = make(map[string]*zap.SugaredLogger)

var levels = make(map[string]zap.AtomicLevel)
var defaultLevel = zapcore.InfoLevel
var output = zapcore.AddSync(os.Stdout)

var logCore = newCore(ColorizedOutput, output, defaultLevel)

/**
func newLogger(options []zap.Option) (*zap.Logger, error) {
	var level zapcore.Level
	err := (&level).UnmarshalText([]byte(*loggerLevelPtr))
	if err != nil {
		return nil, err
	}

	conf := zap.NewProductionConfig()

	// Use logger profile if set on command line before falling back
	// to default based on build type.
	switch *loggerProfilePtr {
	case "dev":
		conf = zap.NewDevelopmentConfig()
	case "prod":
		conf = zap.NewProductionConfig()
	default:
		if version.IsDevBuild() {
			conf = zap.NewDevelopmentConfig()
		}
	}

	conf.Encoding = *loggerFormatPtr
	if conf.Encoding == "console" {
		// Human-readable timestamps for console format of logs.
		conf.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	}

	conf.Level.SetLevel(level)
	return conf.Build(options...)
}

*/

var DefaultLogger = GetLogger("sharding-proxy")

func GetLogger(name string) *zap.SugaredLogger {
	loggerMutex.Lock()
	defer loggerMutex.Unlock()
	log, ok := loggers[name]
	if !ok {
		levels[name] = zap.NewAtomicLevelAt(defaultLevel)

		log = zap.New(logCore, zap.AddCaller()).
			WithOptions(zap.IncreaseLevel(levels[name])).
			Named(name).
			Sugar()

		loggers[name] = log
	}

	return log
}
