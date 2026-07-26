package cli

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/resultpackagefiles"
)

type staticResultPackageAvailabilityLookup struct {
	result  resultpackagefiles.LookupAvailabilityResult
	err     error
	request resultpackagefiles.LookupAvailabilityRequest
}

func (s *staticResultPackageAvailabilityLookup) LookupResultPackageAvailability(
	_ context.Context,
	request resultpackagefiles.LookupAvailabilityRequest,
) (resultpackagefiles.LookupAvailabilityResult, error) {
	s.request = request
	return s.result, s.err
}

func TestLocalResultPackageAvailabilityProvider(t *testing.T) {
	root := control.PrincipalIdentity{
		ControllerID: "123e4567-e89b-42d3-a456-426614174180",
		TreeID:       "123e4567-e89b-42d3-a456-426614174181",
		AgentID:      "123e4567-e89b-42d3-a456-426614174182",
		DeviceID:     "123e4567-e89b-42d3-a456-426614174183",
	}
	manifest := protocol.ResultManifest{PackageID: "123e4567-e89b-42d3-a456-426614174184"}
	for _, test := range []struct {
		name      string
		result    resultpackagefiles.PackageAvailability
		err       error
		want      protocol.ResultPackageAvailability
		wantError bool
	}{
		{name: "available", result: resultpackagefiles.PackageAvailable, want: protocol.ResultPackageAvailable},
		{name: "evicted", result: resultpackagefiles.PackageEvicted, want: protocol.ResultPackageEvicted},
		{name: "lookup failure", err: errors.New("lookup failed"), wantError: true},
		{name: "invalid manager result", result: "unexpected", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			lookup := &staticResultPackageAvailabilityLookup{
				result: resultpackagefiles.LookupAvailabilityResult{
					PackageID: manifest.PackageID, Availability: test.result,
				},
				err: test.err,
			}
			got, err := (localResultPackageAvailabilityProvider{manager: lookup}).
				LookupResultPackageAvailability(
					context.Background(),
					localbridge.ResultPackageAvailabilityLookup{Root: root, Manifest: manifest},
				)
			if (err != nil) != test.wantError || got != test.want {
				t.Fatalf("availability = %q, error %v", got, err)
			}
			wantRequest := resultpackagefiles.LookupAvailabilityRequest{Root: root, Manifest: manifest}
			if !reflect.DeepEqual(lookup.request, wantRequest) {
				t.Fatalf("lookup request = %#v, want %#v", lookup.request, wantRequest)
			}
		})
	}
}
