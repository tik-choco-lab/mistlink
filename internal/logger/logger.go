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

var (
	log  *zap.Logger = zap.NewNop()
	once sync.Once
)

type Options struct {
	Debug     bool
	UseStderr bool
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

		if opts.Debug || opts.UseStderr {
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
}

func Debugf(category, format string, args ...interface{}) {
	log.Debug(fmt.Sprintf(format, args...), zap.String("category", category))
}

func Info(category, message string) {
	log.Info(message, zap.String("category", category))
}

func Infof(category, format string, args ...interface{}) {
	log.Info(fmt.Sprintf(format, args...), zap.String("category", category))
}

func Warn(category, message string) {
	log.Warn(message, zap.String("category", category))
}

func Warnf(category, format string, args ...interface{}) {
	log.Warn(fmt.Sprintf(format, args...), zap.String("category", category))
}

func Error(category, message string) {
	log.Error(message, zap.String("category", category))
}

func Errorf(category, format string, args ...interface{}) {
	log.Error(fmt.Sprintf(format, args...), zap.String("category", category))
}

func LogError(category string, err error) {
	if err != nil {
		log.Error(err.Error(), zap.String("category", category))
	}
}
