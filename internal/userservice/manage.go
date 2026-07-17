package userservice

type Result struct {
	State    State
	Kind     Kind
	Artifact string
}

func Install(binaryPath, configPath string) (Result, error) {
	return platformInstall(binaryPath, configPath)
}
