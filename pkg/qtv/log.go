package qtv

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

//
// Log handling.
//

func (qtv *QTV) regLogVars() {
	qtv.qvs.RegEx("log_level", "info", 0, logLevelOnChange)
	qtv.qvs.RegEx("log_timeformat", "2006-01-02T15:04:05.000", 0, logTimeFormatOnChange)
	qtv.qvs.RegEx("log_pretty", "1", 0, logPrettyOnChange)
}

func logLevelOnChange(qtv *QTV, name string, cur *qVar, new *qVar) (allowed bool) {
	if level, err := zerolog.ParseLevel(new.Str); err != nil {
		log.Err(err).Str("ctx", "log").Msg("")
		return false
	} else {
		zerolog.SetGlobalLevel(level)
	}
	return true
}

func logTimeFormatOnChange(qtv *QTV, name string, cur *qVar, new *qVar) (allowed bool) {
	zerolog.TimeFieldFormat = new.Str
	// If we using pretty logger then we have to apply time format for it.
	if logPretty := qtv.qvs.Find("log_pretty"); logPretty != nil && logPretty.Bool {
		logSetPrettyOutput()
	}
	return true
}

func logPrettyOnChange(qtv *QTV, name string, cur *qVar, new *qVar) (allowed bool) {
	// Switch between pretty and normal logger.
	if new.Bool {
		logSetPrettyOutput()
	} else {
		log.Logger = log.Output(os.Stderr)
	}
	return true
}

func logSetPrettyOutput() {
	// Create pretty writer.
	prettyOutPut := zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: zerolog.TimeFieldFormat,
	}
	// Set it as output.
	log.Logger = log.Output(prettyOutPut)
}
