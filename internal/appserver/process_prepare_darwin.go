//go:build darwin

package appserver

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const darwinSupervisorForceByte = 1

type darwinSupervisorOwner struct {
	mu     sync.Mutex
	reader *os.File
	writer *os.File
}

func preparePlatformOwnedProcess(
	command *exec.Cmd,
	supervisorBinary string,
	closeTimeout time.Duration,
) (ownedProcess, error) {
	target, err := json.Marshal(darwinSupervisorTarget{
		Binary: command.Path,
		Args:   append([]string(nil), command.Args[1:]...),
	})
	if err != nil {
		return nil, err
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	watchdogFD := 3 + len(command.ExtraFiles)
	command.ExtraFiles = append(command.ExtraFiles, reader)
	command.Path = supervisorBinary
	command.Args = []string{supervisorBinary, darwinSupervisorArgument}
	command.Env = setEnvironment(command.Env, darwinSupervisorModeEnvironment, "1")
	command.Env = setEnvironment(command.Env, darwinSupervisorTargetEnvironment, string(target))
	command.Env = setEnvironment(
		command.Env,
		darwinSupervisorWatchdogEnvironment,
		strconv.Itoa(watchdogFD),
	)
	command.Env = setEnvironment(
		command.Env,
		darwinSupervisorTimeoutEnvironment,
		closeTimeout.String(),
	)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return &darwinSupervisorOwner{
		reader: reader, writer: writer,
	}, nil
}

func (o *darwinSupervisorOwner) Attach(*os.Process) error {
	o.mu.Lock()
	reader := o.reader
	o.reader = nil
	o.mu.Unlock()
	if reader == nil {
		return nil
	}
	return reader.Close()
}

func (o *darwinSupervisorOwner) Wait(command *exec.Cmd) processWaitResult {
	waitErr := command.Wait()
	closeErr := o.closeWatchdog()
	var exitError *exec.ExitError
	if errors.As(waitErr, &exitError) && exitError.ExitCode() == darwinSupervisorCleanupExitCode {
		return processWaitResult{
			exitErr: closeErr,
			cleanupErr: errors.New(
				"Darwin app-server supervisor could not confirm descendant cleanup",
			),
		}
	}
	return processWaitResult{exitErr: errors.Join(waitErr, closeErr)}
}

func (o *darwinSupervisorOwner) Terminate() error {
	return o.forceWatchdog()
}

func (o *darwinSupervisorOwner) forceWatchdog() error {
	o.mu.Lock()
	reader := o.reader
	writer := o.writer
	o.reader = nil
	o.writer = nil
	o.mu.Unlock()
	var err error
	if writer != nil {
		count, writeErr := writer.Write([]byte{darwinSupervisorForceByte})
		if writeErr == nil && count != 1 {
			writeErr = errors.New("short Darwin supervisor force write")
		}
		err = errors.Join(err, writeErr, writer.Close())
	}
	if reader != nil {
		err = errors.Join(err, reader.Close())
	}
	return err
}

func (o *darwinSupervisorOwner) closeWatchdog() error {
	o.mu.Lock()
	reader := o.reader
	writer := o.writer
	o.reader = nil
	o.writer = nil
	o.mu.Unlock()
	var err error
	if writer != nil {
		err = errors.Join(err, writer.Close())
	}
	if reader != nil {
		err = errors.Join(err, reader.Close())
	}
	return err
}
