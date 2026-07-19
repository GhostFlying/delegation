package userservice

type Result struct {
	State    State
	Kind     Kind
	Artifact string
	Role     ServiceRole
}

func Install(role ServiceRole, binaryPath, configPath string) (Result, error) {
	result, err := platformPrepare(role, binaryPath, configPath)
	if err != nil {
		return result, err
	}
	return platformActivate(result, binaryPath, configPath)
}

func Prepare(role ServiceRole, binaryPath, configPath string) (Result, error) {
	return platformPrepare(role, binaryPath, configPath)
}
