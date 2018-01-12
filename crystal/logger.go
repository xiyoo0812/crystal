package crystal

import (
	"os"
	"fmt"
	"log"
	"time"
	"path"
	"runtime"
	"sync/atomic"
	"path/filepath"
	"sync"
)

// LogLevel is the log level type.
type LogLevel int
const (
	DEBUG LogLevel = iota		// DEBUG represents debug log level.
	INFO						// INFO represents info log level.
	WARN						// WARN represents warn log level.
	ERROR						// ERROR represents error log level.
	FATAL						// FATAL represents fatal log level.
)

var (
	started 	int32
	logIns  	*Logger
	tagName   = map[LogLevel]string{
		DEBUG: "DEBUG",
		INFO:  "INFO",
		WARN:  "WARN",
		ERROR: "ERROR",
		FATAL: "FATAL",
	}
)

// logSegment implements io.Writer
type logSegment struct {
	unit         time.Duration
	logPath      string
	logFile      *os.File
	mu 			sync.Mutex
	timeToCreate <-chan time.Time
}

func (ls *logSegment) Write(p []byte) (n int, err error) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.timeToCreate != nil {
		select {
		case current := <-ls.timeToCreate:
			fullpath := getLogFilePath(ls.logPath)
			err := os.MkdirAll(fullpath, os.ModePerm)
			if err == nil {
				ls.logFile.Close()
				ls.logFile = nil
				name := getLogFileName(ls.unit)
				ls.logFile, err = os.Create(path.Join(fullpath, name))
				if err != nil {
					// log into stderr if we can't create new file
					fmt.Fprintln(os.Stderr, err)
					ls.logFile = os.Stderr
				} else {
					next := current.Truncate(ls.unit).Add(ls.unit)
					ls.timeToCreate = time.After(next.Sub(time.Now()))
				}
			} else {
				ls.logFile.WriteString(err.Error())
			}
		default:
			// do nothing
		}
	}
	return ls.logFile.Write(p)
}

func (ls *logSegment) Close() {
	ls.logFile.Close()
}

// Logger is the logger type.
type Logger struct {
	logger     	*log.Logger
	level      	LogLevel
	segment    	*logSegment
	stopped    	int32
	logPath    	string
	unit       	time.Duration
	isStdout   	bool
	wg		   	*sync.WaitGroup
	mut 		bool
}

// set the logger mut thread
func (l Logger) MutThread(bm bool) {
	l.mut = bm
}

// Stop stops the logger.
func (l Logger) Stop() {
	if atomic.CompareAndSwapInt32(&l.stopped, 0, 1) {
		l.wg.Wait()
		if l.segment != nil {
			l.segment.Close()
		}
		l.segment = nil
		l.logger = nil
		atomic.StoreInt32(&started, 0)
	}
}

func (l Logger) doPrintf(level LogLevel, format string, v ...interface{}) {
	if level >= l.level {
		if l.logger != nil {
			funcName, fileName, lineNum := getRuntimeInfo()
			realRogger := func(){
				format := fmt.Sprintf("%5s [%s] (%s:%d) - %s", tagName[level], path.Base(funcName), path.Base(fileName), lineNum, format)
				l.logger.Printf(format, v...)
			}
			if l.mut {
				l.wg.Add(1)
				go func(){
					realRogger()
					l.wg.Done()
				}()
			} else {
				realRogger()
			}
		}
		if l.isStdout {
			funcName, fileName, lineNum := getRuntimeInfo()
			realRogger := func(){
				format := fmt.Sprintf("%5s [%s] (%s:%d) - %s", tagName[level], path.Base(funcName), path.Base(fileName), lineNum, format)
				log.Printf(format, v...)
			}
			if l.mut {
				l.wg.Add(1)
				go func(){
					realRogger()
					l.wg.Done()
				}()
			} else {
				realRogger()
			}
		}
	}
}

func (l Logger) doPrintln(level LogLevel, v ...interface{}) {
	if level >= l.level {
		if l.logger != nil {
			funcName, fileName, lineNum := getRuntimeInfo()
			realRogger := func(){
				prefix := fmt.Sprintf("%5s [%s] (%s:%d) - ", tagName[level], path.Base(funcName), path.Base(fileName), lineNum)
				value := fmt.Sprintf("%s%s", prefix, fmt.Sprintln(v...))
				l.logger.Print(value)
			}
			if l.mut {
				l.wg.Add(1)
				go func(){
					realRogger()
					l.wg.Done()
				}()
			} else {
				realRogger()
			}
		}
		if l.isStdout {
			funcName, fileName, lineNum := getRuntimeInfo()
			realRogger := func(){
				prefix := fmt.Sprintf("%5s [%s] (%s:%d) - ", tagName[level], path.Base(funcName), path.Base(fileName), lineNum)
				value := fmt.Sprintf("%s%s", prefix, fmt.Sprintln(v...))
				log.Print(value)
			}
			if l.mut {
				l.wg.Add(1)
				go func(){
					realRogger()
					l.wg.Done()
				}()
			} else {
				realRogger()
			}
		}
	}
}

// Init returns a decorated innerLogger.
func InitLogger(level LogLevel, unit time.Duration, path string, stdout bool) *Logger {
	if atomic.CompareAndSwapInt32(&started, 0, 1) {
		logIns = &Logger{}
		logIns.mut = false
		logIns.unit = unit
		logIns.level = level
		logIns.isStdout = stdout
		logIns.wg = &sync.WaitGroup{}
		if len(path) > 0 {
			logIns.logPath = path
			logIns.segment = newLogSegment(logIns.unit, path)
			if logIns.segment != nil {
				logIns.logger = log.New(logIns.segment, "", log.LstdFlags)
			}
		}
		return logIns
	}
	panic("InitLogger() already called")
}

func newLogSegment(unit time.Duration, logPath string) *logSegment {
	if logPath != "" {
		fullpath := getLogFilePath(logPath)
		err := os.MkdirAll(fullpath, os.ModePerm)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return nil
		}
		if unit == 0 { unit = time.Hour * 24 }
		name := getLogFileName(unit)
		logFile, err := os.OpenFile(path.Join(fullpath, name), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			if os.IsNotExist(err) {
				logFile, err = os.Create(path.Join(logPath, name))
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					return nil
				}
			} else {
				fmt.Fprintln(os.Stderr, err)
				return nil
			}
		}
		now := time.Now()
		next := now.Truncate(unit).Add(unit)
		var timeToCreate <-chan time.Time
		timeToCreate = time.After(next.Sub(now))
		return &logSegment{
			logPath:      logPath,
			logFile:      logFile,
			timeToCreate: timeToCreate,
			unit:         unit,
		}
	}
	return nil
}

func getLogFilePath(p string) string {
	now := time.Now()
	proc := filepath.Base(os.Args[0])
	year := now.Year()
	month := now.Month()
	day := now.Day()
	return path.Join(p, proc, fmt.Sprintf("%04d-%02d-%02d", year, month, day))
}

func getLogFileName(uint time.Duration) string {
	now := time.Now()
	year := now.Year()
	month := now.Month()
	day := now.Day()
	hour := now.Hour()
	if uint == time.Hour {
		return fmt.Sprintf("%04d-%02d-%02d-%02d.log", year, month, day, hour)
	} else if uint == time.Minute {
		minute := now.Minute()
		return fmt.Sprintf("%04d-%02d-%02d-%02d-%02d.log", year, month, day, hour, minute)
	} else {
		return fmt.Sprintf("%04d-%02d-%02d.log", year, month, day)
	}
}

func getRuntimeInfo() (string, string, int) {
	pc, fn, ln, ok := runtime.Caller(3) // 3 steps up the stack frame
	if !ok {
		fn = "???"
		ln = 0
	}
	function := "???"
	caller := runtime.FuncForPC(pc)
	if caller != nil {
		function = caller.Name()
	}
	return function, fn, ln
}

// Debugf prints formatted debug log.
func Debugf(format string, v ...interface{}) {
	logIns.doPrintf(DEBUG, format, v...)
}

// Infof prints formatted info log.
func Infof(format string, v ...interface{}) {
	logIns.doPrintf(INFO, format, v...)
}

// Warnf prints formatted warn log.
func Warnf(format string, v ...interface{}) {
	logIns.doPrintf(WARN, format, v...)
}

// Errorf prints formatted error log.
func Errorf(format string, v ...interface{}) {
	logIns.doPrintf(ERROR, format, v...)
}

// Fatalf prints formatted fatal log and exits.
func Fatalf(format string, v ...interface{}) {
	go logIns.doPrintf(FATAL, format, v...)
	os.Exit(1)
}

// Debugln prints debug log.
func Debugln(v ...interface{}) {
	logIns.doPrintln(DEBUG, v...)
}

// Infoln prints info log.
func Infoln(v ...interface{}) {
	logIns.doPrintln(INFO, v...)
}

// Warnln prints warn log.
func Warnln(v ...interface{}) {
	logIns.doPrintln(WARN, v...)
}

// Errorln prints error log.
func Errorln(v ...interface{}) {
	logIns.doPrintln(ERROR, v...)
}

// Fatalln prints fatal log and exits.
func Fatalln(v ...interface{}) {
	logIns.doPrintln(FATAL, v...)
	os.Exit(1)
}
