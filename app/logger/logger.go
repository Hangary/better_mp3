/*
This package contains logger-related utility.
*/
package logger

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

var (
	// InfoLogger : logging for info messages
	InfoLogger *log.Logger

	// WarningLogger : logging for warning messages
	WarningLogger *log.Logger

	// ErrorLogger : logging for error messages
	ErrorLogger *log.Logger

	// ErrorLogger : logging for error messages
	DebugLogger *log.Logger

	LogFile *os.File

	TimeFormat = "2006-01-02 15:04:05"
)

func init() {
	LogFile, err := os.OpenFile("logs.txt", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}

	InfoLogger 		= log.New(LogFile, "[INFO]", log.Ldate|log.Ltime|log.Lshortfile)
	WarningLogger 	= log.New(LogFile, "[WARNING]", log.Ldate|log.Ltime|log.Lshortfile)
	ErrorLogger 	= log.New(LogFile, "[ERROR]", log.Ldate|log.Ltime|log.Lshortfile)
	DebugLogger 	= log.New(LogFile, "[DEBUG]", log.Ldate|log.Ltime|log.Lshortfile)
}

func getTimeString() string {
	return "(" + strings.Split(time.Now().Format(TimeFormat), " ")[1] + ")"
}

func PrintToConsole(args ...interface{}) {
	fmt.Print(blue(fmt.Sprintln(args...)))
}

func PrintInfo(args ...interface{}) {
	InfoLogger.Println(args)

	fmt.Print(green("[INFO] " + getTimeString() + " "))
	fmt.Print(green(fmt.Sprintln(args...)))
}

func PrintWarning(args ...interface{}) {
	WarningLogger.Println(args)

	fmt.Print(yellow("[WARN] " + getTimeString() + " "))
	fmt.Print(yellow(fmt.Sprintln(args...)))
}

func PrintError(args ...interface{}) {
	ErrorLogger.Println(args)

	fmt.Print(red("[ERROR]" + getTimeString() + " "))
	fmt.Print(red(fmt.Sprintln(args...)))
}

func PrintDebug(args ...interface{}) {
	ErrorLogger.Println(args)
	fmt.Print(yellow("[DEBUG]" + getTimeString() + " "))
	fmt.Println(args...)
}
