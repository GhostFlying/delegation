package instanceid

import "errors"

func Validate(value string) error {
	if len(value) < 1 || len(value) > 32 {
		return errors.New("instanceId must contain 1 through 32 characters")
	}
	for index, character := range value {
		valid := character >= 'a' && character <= 'z'
		if index > 0 {
			valid = valid || character >= '0' && character <= '9' || character == '-'
		}
		if !valid {
			return errors.New("instanceId must start with a lowercase letter and use only lowercase letters, digits, or hyphens")
		}
	}
	if value[len(value)-1] == '-' {
		return errors.New("instanceId must not end with a hyphen")
	}
	return nil
}
