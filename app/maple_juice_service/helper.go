package maple_juice_service

import (
	"fmt"
	"os/exec"
)

func executeFile(execFileName string, inputFileName string, outputFileName string) error {
	cmd := fmt.Sprintf("./%s < %s > %s", execFileName, inputFileName, outputFileName)
	_, err := exec.Command("/bin/sh", "-c", cmd).Output()
	if err != nil {
		//fmt.Printf("%s", err)
		return err
	}
	return nil
}