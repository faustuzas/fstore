package util

import (
	"bufio"
	"os"
	"os/signal"

	"github.com/faustuzas/distributed-kv/logging"
)

func PressEnterToExit() {
	_, _, _ = bufio.NewReader(os.Stdin).ReadLine()
}

func WaitForTerminationRequest() <-chan struct{} {
	stopProgramCh := make(chan struct{})
	go func() {
		// wait for user to click Enter key to finish the program
		PressEnterToExit()

		logging.System.Infof("User requested termination by clicking Enter")

		close(stopProgramCh)
	}()

	interruptCh := make(chan os.Signal)
	signal.Notify(interruptCh, os.Interrupt)

	go func() {
		select {
		case <-interruptCh:
			logging.System.Infof("User requested termination by CTRL+C")
			close(stopProgramCh)
		case <-stopProgramCh:
			return
		}
	}()

	return stopProgramCh
}
