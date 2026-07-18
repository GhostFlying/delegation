package userservice

type Result struct {
	State    State
	Kind     Kind
	Artifact string
}

func Install(binaryPath, configPath string) (Result, error) {
	result, err := platformPrepare(binaryPath, configPath)
	if err != nil {
		return result, err
	}
	return platformActivate(result, binaryPath, configPath)
}

func Prepare(binaryPath, configPath string) (Result, error) {
	return platformPrepare(binaryPath, configPath)
}
