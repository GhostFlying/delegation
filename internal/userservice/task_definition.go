package userservice

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

type taskDefinition struct {
	Description     string
	URI             string
	Enabled         bool
	TriggerUserID   string
	PrincipalUserID string
	Triggers        string
	Principals      string
	Settings        string
	Actions         string
}

func taskOwned(task taskDefinition) bool {
	return task.Description == Marker && task.URI == ScheduledTask
}

func taskDefinitionsEquivalent(
	desired taskDefinition,
	existing taskDefinition,
	userIDsEqual func(string, string) (bool, error),
) (bool, error) {
	if desired.Description != existing.Description || desired.URI != existing.URI ||
		desired.Enabled != existing.Enabled ||
		desired.Triggers != existing.Triggers || desired.Principals != existing.Principals ||
		desired.Settings != existing.Settings || desired.Actions != existing.Actions {
		return false, nil
	}
	if userIDsEqual == nil {
		return false, errors.New("scheduled task user identity comparer is required")
	}
	for _, pair := range [][2]string{
		{desired.TriggerUserID, desired.PrincipalUserID},
		{desired.TriggerUserID, existing.TriggerUserID},
		{desired.PrincipalUserID, existing.PrincipalUserID},
	} {
		equal, err := userIDsEqual(pair[0], pair[1])
		if err != nil {
			return false, err
		}
		if !equal {
			return false, nil
		}
	}
	return true, nil
}

func canonicalizeTaskComponent(name string, child taskXMLNode) (string, string, error) {
	var userID string
	switch name {
	case "Triggers":
		logonTrigger, err := optionalUniqueTaskChild(&child, "LogonTrigger")
		if err != nil {
			return "", "", err
		}
		if logonTrigger != nil {
			for _, taskDefault := range [][2]string{
				{"Enabled", "true"},
				{"ExecutionTimeLimit", "PT72H"},
				{"Delay", "PT0M"},
			} {
				if err := omitDefaultTaskLeaf(logonTrigger, taskDefault[0], taskDefault[1]); err != nil {
					return "", "", err
				}
			}
			userID, err = redactOptionalTaskLeaf(logonTrigger, "UserId")
			if err != nil {
				return "", "", err
			}
		}
	case "Principals":
		principal, err := optionalUniqueTaskChild(&child, "Principal")
		if err != nil {
			return "", "", err
		}
		if principal != nil {
			if err := omitDefaultTaskLeaf(principal, "RunLevel", "LeastPrivilege"); err != nil {
				return "", "", err
			}
			userID, err = redactOptionalTaskLeaf(principal, "UserId")
			if err != nil {
				return "", "", err
			}
			sortTaskChildren(principal)
		}
	case "Settings":
		if err := normalizeTaskSettings(&child); err != nil {
			return "", "", err
		}
	case "Actions":
	default:
		return "", "", fmt.Errorf("scheduled task XML has unknown canonical component %s", name)
	}
	encoded, err := json.Marshal(child)
	if err != nil {
		return "", "", fmt.Errorf("canonicalize scheduled task %s: %w", name, err)
	}
	return string(encoded), userID, nil
}

func normalizeTaskSettings(settings *taskXMLNode) error {
	for _, taskDefault := range [][2]string{
		{"AllowStartOnDemand", "true"},
		{"MultipleInstancesPolicy", "IgnoreNew"},
		{"DisallowStartIfOnBatteries", "true"},
		{"StopIfGoingOnBatteries", "true"},
		{"AllowHardTerminate", "true"},
		{"StartWhenAvailable", "false"},
		{"RunOnlyIfNetworkAvailable", "false"},
		{"WakeToRun", "false"},
		{"Hidden", "false"},
		{"DeleteExpiredTaskAfter", "PT0S"},
		{"ExecutionTimeLimit", "PT72H"},
		{"Priority", "7"},
		{"RunOnlyIfIdle", "false"},
		{"UseUnifiedSchedulingEngine", "false"},
		{"DisallowStartOnRemoteAppSession", "false"},
	} {
		if err := omitDefaultTaskLeaf(settings, taskDefault[0], taskDefault[1]); err != nil {
			return err
		}
	}
	if err := omitTaskLeaf(settings, "Enabled"); err != nil {
		return err
	}
	idleSettings, err := optionalUniqueTaskChild(settings, "IdleSettings")
	if err != nil {
		return err
	}
	if idleSettings != nil {
		for _, taskDefault := range [][2]string{
			{"Duration", "PT10M"},
			{"WaitTimeout", "PT1H"},
			{"StopOnIdleEnd", "true"},
			{"RestartOnIdle", "false"},
		} {
			if err := omitDefaultTaskLeaf(idleSettings, taskDefault[0], taskDefault[1]); err != nil {
				return err
			}
		}
		sortTaskChildren(idleSettings)
	}
	sortTaskChildren(settings)
	return nil
}

func omitTaskLeaf(parent *taskXMLNode, name string) error {
	child, err := optionalUniqueTaskChild(parent, name)
	if err != nil || child == nil {
		return err
	}
	if len(child.Attributes) != 0 || len(child.Children) != 0 {
		return fmt.Errorf("scheduled task XML %s is not a scalar", name)
	}
	for index := range parent.Children {
		if &parent.Children[index] == child {
			parent.Children = append(parent.Children[:index], parent.Children[index+1:]...)
			return nil
		}
	}
	return errors.New("scheduled task XML field disappeared during normalization")
}

func redactOptionalTaskLeaf(parent *taskXMLNode, name string) (string, error) {
	child, err := optionalUniqueTaskChild(parent, name)
	if err != nil || child == nil {
		return "", err
	}
	if len(child.Attributes) != 0 || len(child.Children) != 0 {
		return "", fmt.Errorf("scheduled task XML %s is not a scalar", name)
	}
	value := child.Text
	child.Text = ""
	return value, nil
}

func omitDefaultTaskLeaf(parent *taskXMLNode, name, value string) error {
	child, err := optionalUniqueTaskChild(parent, name)
	if err != nil || child == nil {
		return err
	}
	if len(child.Attributes) != 0 || len(child.Children) != 0 {
		return fmt.Errorf("scheduled task XML %s is not a scalar", name)
	}
	if child.Text != value {
		return nil
	}
	for i := range parent.Children {
		if &parent.Children[i] == child {
			parent.Children = append(parent.Children[:i], parent.Children[i+1:]...)
			return nil
		}
	}
	return errors.New("scheduled task XML default field disappeared during normalization")
}

func sortTaskChildren(parent *taskXMLNode) {
	sort.SliceStable(parent.Children, func(i, j int) bool {
		left := parent.Children[i]
		right := parent.Children[j]
		if left.Space != right.Space {
			return left.Space < right.Space
		}
		return left.Local < right.Local
	})
}
