package userservice

type Result struct {
	State    State
	Kind     Kind
	Artifact string
	Role     ServiceRole
}

func Install(role ServiceRole, invocation Invocation) (Result, error) {
	result, err := platformPrepare(role, invocation)
	if err != nil {
		return result, err
	}
	return platformActivate(result, invocation)
}

func Prepare(role ServiceRole, invocation Invocation) (Result, error) {
	return platformPrepare(role, invocation)
}
