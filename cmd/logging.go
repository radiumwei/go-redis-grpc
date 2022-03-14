package cmd

import (
	"bytes"
	"os"
	"strings"
	"time"

	"fmt"
	"path/filepath"
	"strconv"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

type LogFormatter struct{}

func (m *LogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	timestamp := entry.Time.Format("2006-01-02 15:04:05.000")
	newLog := fmt.Sprintf("[%s] [%s] %s\n", timestamp, fmt.Sprintf("%-5s", strings.ToUpper(entry.Level.String())), entry.Message)

	b.WriteString(newLog)
	return b.Bytes(), nil
}

//initialize log format
func InitLog(saveLog bool) {
	curPid := strconv.Itoa(os.Getpid())

	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exePath := filepath.Dir(ex)

	if err := os.MkdirAll(exePath+"/log/", os.ModePerm); err != nil {
		panic(err)
	}

	if saveLog {
		path := exePath + "/log/pid-" + curPid + ".log"
		writer, _ := rotatelogs.New(
			path,
			rotatelogs.WithRotationTime(time.Duration(24)*time.Hour),
			rotatelogs.WithRotationCount(7),
		)

		log.SetOutput(writer)
	}

	log.SetFormatter(&LogFormatter{})
}
