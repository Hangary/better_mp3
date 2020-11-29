package logger
/*
This file used to add color for log.
Credit: codes come from https://toutiao.io/posts/2889gp/preview.
*/
import "fmt"

const (
	color_red = uint8(iota + 91)
	color_green
	color_yellow
	color_blue
)

func red(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_red, s)
}
func green(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_green, s)
}
func yellow(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_yellow, s)
}
func blue(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_blue, s)
}