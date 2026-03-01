package logger

import (
	"fmt"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Options struct {
	Debug          bool
	UseStderr      bool
	DisableConsole bool
}

type LogEntry struct {
	Level    string
	Category string
	Message  string
	Time     time.Time
}

var (
	log           *zap.Logger = zap.NewNop()
	once          sync.Once
	logHandlers   []func(LogEntry)
	logHandlersMu sync.RWMutex
)

func AddLogHandler(h func(LogEntry)) {
	logHandlersMu.Lock()
	defer logHandlersMu.Unlock()
	logHandlers = append(logHandlers, h)
}

func notifyHandlers(level, category, message string) {
	entry := LogEntry{
		Level:    level,
		Category: category,
		Message:  message,
		Time:     time.Now(),
	}
	logHandlersMu.RLock()
	defer logHandlersMu.RUnlock()
	for _, h := range logHandlers {
		h(entry)
	}
}

func Init()                    { InitWithOptions(Options{}) }
func InitWithDebug(debug bool) { InitWithOptions(Options{Debug: debug}) }

func InitWithOptions(opts Options) {
	once.Do(func() {
		lumberJackLogger := &lumberjack.Logger{
			Filename:   "./logs/app.log",
			MaxSize:    10,
			MaxBackups: 3,
			MaxAge:     28,
			Compress:   true,
		}

		logLevel := zap.InfoLevel
		if opts.Debug {
			logLevel = zap.DebugLevel
		}

		fileEncoderConfig := zap.NewProductionEncoderConfig()
		fileEncoderConfig.TimeKey = "timestamp"
		fileEncoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			jst := time.FixedZone("Asia/Tokyo", 9*60*60)
			enc.AppendString(t.In(jst).Format(time.RFC3339))
		}

		fileCore := zapcore.NewCore(
			zapcore.NewJSONEncoder(fileEncoderConfig),
			zapcore.AddSync(lumberJackLogger),
			logLevel,
		)

		var cores []zapcore.Core
		cores = append(cores, fileCore)

		if !opts.DisableConsole && (opts.Debug || opts.UseStderr) {
			consoleEncoderConfig := zap.NewDevelopmentEncoderConfig()
			consoleEncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
			consoleEncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("15:04:05")

			output := os.Stdout
			if opts.UseStderr {
				output = os.Stderr
			}

			consoleCore := zapcore.NewCore(
				zapcore.NewConsoleEncoder(consoleEncoderConfig),
				zapcore.AddSync(output),
				logLevel,
			)
			cores = append(cores, consoleCore)
		}

		log = zap.New(zapcore.NewTee(cores...), zap.AddCaller(), zap.AddCallerSkip(1))
	})
}

func Sync() {
	if log != nil {
		_ = log.Sync()
	}
}

func Debug(category, message string) {
	log.Debug(message, zap.String("category", category))
	notifyHandlers("DEBUG", category, message)
}

func Debugf(category, format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	log.Debug(message, zap.String("category", category))
	notifyHandlers("DEBUG", category, message)
}

func Info(category, message string) {
	log.Info(message, zap.String("category", category))
	notifyHandlers("INFO", category, message)
}

func Infof(category, format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	log.Info(message, zap.String("category", category))
	notifyHandlers("INFO", category, message)
}

func Warn(category, message string) {
	log.Warn(message, zap.String("category", category))
	notifyHandlers("WARN", category, message)
}

func Warnf(category, format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	log.Warn(message, zap.String("category", category))
	notifyHandlers("WARN", category, message)
}

func Error(category, message string) {
	log.Error(message, zap.String("category", category))
	notifyHandlers("ERROR", category, message)
}

func Errorf(category, format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	log.Error(message, zap.String("category", category))
	notifyHandlers("ERROR", category, message)
}

func LogError(category string, err error) {
	if err != nil {
		message := err.Error()
		log.Error(message, zap.String("category", category))
		notifyHandlers("ERROR", category, message)
	}
}
