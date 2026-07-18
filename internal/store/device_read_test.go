package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
)

const deviceThirdID = "123e4567-e89b-42d3-a456-426614174032"

const deviceThirdControllerID = "123e4567-e89b-42d3-a456-426614174033"

func TestListDevicesUsesRevisionBoundPages(t *testing.T) {
	registry := openTestStore(t)
	ctx := context.Background()
	for index, deviceID := range []string{testDeviceID, deviceSecondID, deviceThirdID} {
		if _, err := registry.RegisterTrustedDevice(
			ctx,
			deviceDescriptor(testControllerID, deviceID, control.DeviceRoleWorker),
			time.Unix(int64(index+1), 0),
		); err != nil {
			t.Fatal(err)
		}
	}
	first, err := registry.ListDevices(ctx, testControllerID, DevicePageRequest{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision != 3 || len(first.Devices) != 2 ||
		first.Devices[0].DeviceID != testDeviceID ||
		first.Devices[1].DeviceID != deviceSecondID ||
		first.NextCursor != deviceSecondID {
		t.Fatalf("first device page = %#v", first)
	}
	second, err := registry.ListDevices(ctx, testControllerID, DevicePageRequest{
		AfterDeviceID:    first.NextCursor,
		Limit:            2,
		ExpectedRevision: &first.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Revision != 3 || len(second.Devices) != 1 ||
		second.Devices[0].DeviceID != deviceThirdID || second.NextCursor != "" {
		t.Fatalf("second device page = %#v", second)
	}
	if _, err := registry.HeartbeatDevice(
		ctx, testControllerID, deviceThirdID, 3, time.Unix(4, 0),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ListDevices(ctx, testControllerID, DevicePageRequest{
		AfterDeviceID:    first.NextCursor,
		Limit:            2,
		ExpectedRevision: &first.Revision,
	}); !errors.Is(err, ErrRevisionChanged) {
		t.Fatalf("revision drift error = %v, want ErrRevisionChanged", err)
	}
	currentRevision := uint64(4)
	if retry, err := registry.ListDevices(ctx, testControllerID, DevicePageRequest{
		AfterDeviceID:    first.NextCursor,
		Limit:            2,
		ExpectedRevision: &currentRevision,
	}); err != nil || retry.Revision != 4 || len(retry.Devices) != 1 {
		t.Fatalf("revision-bound retry = %#v, error %v", retry, err)
	}
}

func TestDescribeDeviceReturnsSameSnapshotRevision(t *testing.T) {
	registry := openTestStore(t)
	ctx := context.Background()
	want, err := registry.RegisterTrustedDevice(
		ctx,
		deviceDescriptor(testControllerID, testDeviceID, control.DeviceRoleController),
		time.Unix(1, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	record, err := registry.DescribeDevice(ctx, testControllerID, testDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	if record.RegistryRevision != 1 || !reflect.DeepEqual(record.Device, want) {
		t.Fatalf("described device = %#v, want revision 1 and %#v", record, want)
	}
	if _, err := registry.DescribeDevice(ctx, deviceSecondControllerID, testDeviceID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-controller describe error = %v, want ErrNotFound", err)
	}
}

func TestListDevicesValidatesPageBounds(t *testing.T) {
	registry := openTestStore(t)
	ctx := context.Background()
	for _, request := range []DevicePageRequest{
		{},
		{Limit: MaximumDevicePage + 1},
		{Limit: 1, AfterDeviceID: "not-a-uuid"},
		{Limit: 1, AfterDeviceID: testDeviceID},
	} {
		if _, err := registry.ListDevices(ctx, testControllerID, request); err == nil {
			t.Fatalf("ListDevices accepted request %#v", request)
		}
	}
}

func TestDeviceReadsHandleExactEmptyAndControllerIsolation(t *testing.T) {
	registry := openTestStore(t)
	ctx := context.Background()
	for _, controllerID := range []string{testControllerID, deviceSecondControllerID} {
		for _, deviceID := range []string{testDeviceID, deviceSecondID} {
			descriptor := deviceDescriptor(controllerID, deviceID, control.DeviceRoleWorker)
			descriptor.Name = controllerID
			if _, err := registry.RegisterTrustedDevice(ctx, descriptor, time.Unix(1, 0)); err != nil {
				t.Fatal(err)
			}
		}
	}
	page, err := registry.ListDevices(ctx, testControllerID, DevicePageRequest{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if page.Revision != 2 || len(page.Devices) != 2 || page.NextCursor != "" {
		t.Fatalf("exact-size device page = %#v", page)
	}
	for _, device := range page.Devices {
		if device.ControllerID != testControllerID || device.Name != testControllerID {
			t.Fatalf("list leaked another controller device: %#v", device)
		}
	}
	record, err := registry.DescribeDevice(ctx, deviceSecondControllerID, testDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	if record.RegistryRevision != 2 || record.Device.ControllerID != deviceSecondControllerID ||
		record.Device.Name != deviceSecondControllerID {
		t.Fatalf("isolated device record = %#v", record)
	}
	empty, err := registry.ListDevices(ctx, deviceThirdControllerID, DevicePageRequest{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if empty.Revision != 0 || len(empty.Devices) != 0 || empty.NextCursor != "" {
		t.Fatalf("empty device page = %#v", empty)
	}
	if _, err := registry.DescribeDevice(ctx, deviceThirdControllerID, testDeviceID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty describe error = %v, want ErrNotFound", err)
	}
}

func TestDeviceReadsRecoverAfterCorruptStoredMetadata(t *testing.T) {
	registry := openTestStore(t)
	ctx := context.Background()
	if _, err := registry.RegisterTrustedDevice(
		ctx,
		deviceDescriptor(testControllerID, testDeviceID, control.DeviceRoleWorker),
		time.Unix(1, 0),
	); err != nil {
		t.Fatal(err)
	}
	for _, corrupted := range []string{"{", strings.Repeat("x", maximumFeaturesJSON+1)} {
		if _, err := registry.db.Exec(`
UPDATE devices SET features_json = ? WHERE controller_id = ? AND device_id = ?
`, corrupted, testControllerID, testDeviceID); err != nil {
			t.Fatal(err)
		}
		if _, err := registry.ListDevices(ctx, testControllerID, DevicePageRequest{Limit: 1}); err == nil {
			t.Fatal("list accepted corrupt stored features")
		}
		if _, err := registry.DescribeDevice(ctx, testControllerID, testDeviceID); err == nil {
			t.Fatal("describe accepted corrupt stored features")
		}
	}
	if _, err := registry.db.Exec(`
UPDATE devices SET features_json = '[]' WHERE controller_id = ? AND device_id = ?
`, testControllerID, testDeviceID); err != nil {
		t.Fatal(err)
	}
	if page, err := registry.ListDevices(ctx, testControllerID, DevicePageRequest{Limit: 1}); err != nil || len(page.Devices) != 1 {
		t.Fatalf("list after metadata repair = %#v, error %v", page, err)
	}
	if _, err := registry.DescribeDevice(ctx, testControllerID, testDeviceID); err != nil {
		t.Fatalf("describe after metadata repair: %v", err)
	}
}

func TestConcurrentDeviceReadsUseSingleRevisionSnapshots(t *testing.T) {
	registry := openTestStore(t)
	ctx := context.Background()
	device, err := registry.RegisterTrustedDevice(
		ctx,
		deviceDescriptor(testControllerID, testDeviceID, control.DeviceRoleWorker),
		time.Unix(1, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	writerDone := make(chan error, 1)
	go func() {
		current := device
		for second := int64(2); second <= 300; second++ {
			var err error
			current, err = registry.HeartbeatDevice(
				ctx, testControllerID, testDeviceID, current.Revision, time.Unix(second, 0),
			)
			if err != nil {
				writerDone <- err
				return
			}
		}
		writerDone <- nil
	}()
	var readErr error
	for range 300 {
		page, err := registry.ListDevices(ctx, testControllerID, DevicePageRequest{Limit: 1})
		if err != nil {
			readErr = err
			break
		}
		if len(page.Devices) != 1 || page.Devices[0].Revision > page.Revision {
			readErr = fmt.Errorf("inconsistent device page = %#v", page)
			break
		}
		record, err := registry.DescribeDevice(ctx, testControllerID, testDeviceID)
		if err != nil {
			readErr = err
			break
		}
		if record.Device.Revision > record.RegistryRevision {
			readErr = fmt.Errorf("inconsistent device record = %#v", record)
			break
		}
	}
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
}
