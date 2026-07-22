//go:build !darwin

package appserver

func RunDarwinSupervisorIfRequested() (bool, int) {
	return false, 0
}
