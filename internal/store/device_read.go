package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
)

// MaximumDevicePage keeps a page of maximum-size descriptors below the wire envelope limit.
const MaximumDevicePage = 32

var ErrRevisionChanged = errors.New("device registry revision changed")

type DevicePageRequest struct {
	AfterDeviceID    string
	Limit            int
	ExpectedRevision *uint64
}

type DevicePage struct {
	Revision   uint64
	Devices    []control.Device
	NextCursor string
}

type DeviceRecord struct {
	RegistryRevision uint64
	Device           control.Device
}

func (s *Store) ListDevices(ctx context.Context, controllerID string, request DevicePageRequest) (DevicePage, error) {
	if err := identity.ValidateID(controllerID); err != nil {
		return DevicePage{}, fmt.Errorf("controllerId %w", err)
	}
	if request.AfterDeviceID != "" {
		if err := identity.ValidateID(request.AfterDeviceID); err != nil {
			return DevicePage{}, fmt.Errorf("device cursor %w", err)
		}
		if request.ExpectedRevision == nil {
			return DevicePage{}, errors.New("device cursor requires an expected revision")
		}
	}
	if request.Limit < 1 || request.Limit > MaximumDevicePage {
		return DevicePage{}, fmt.Errorf("device page limit must be from 1 through %d", MaximumDevicePage)
	}
	transaction, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return DevicePage{}, fmt.Errorf("begin device page: %w", err)
	}
	defer transaction.Rollback()
	revision, err := controllerRevision(ctx, transaction, controllerID)
	if err != nil {
		return DevicePage{}, err
	}
	if request.ExpectedRevision != nil && *request.ExpectedRevision != revision {
		return DevicePage{}, ErrRevisionChanged
	}
	rows, err := transaction.QueryContext(ctx, deviceSelect+`
WHERE controller_id = ? AND device_id > ?
ORDER BY device_id
LIMIT ?
`, controllerID, request.AfterDeviceID, request.Limit+1)
	if err != nil {
		return DevicePage{}, fmt.Errorf("list devices: %w", err)
	}
	devices := make([]control.Device, 0, request.Limit)
	for rows.Next() {
		device, err := scanDevice(rows)
		if err != nil {
			rows.Close()
			return DevicePage{}, err
		}
		devices = append(devices, device)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return DevicePage{}, fmt.Errorf("list devices: %w", err)
	}
	if err := rows.Close(); err != nil {
		return DevicePage{}, fmt.Errorf("close device page: %w", err)
	}
	page := DevicePage{Revision: revision, Devices: devices}
	if len(page.Devices) > request.Limit {
		page.NextCursor = page.Devices[request.Limit-1].DeviceID
		page.Devices = page.Devices[:request.Limit]
	}
	if err := transaction.Commit(); err != nil {
		return DevicePage{}, fmt.Errorf("commit device page: %w", err)
	}
	return page, nil
}

func (s *Store) DescribeDevice(ctx context.Context, controllerID, deviceID string) (DeviceRecord, error) {
	if err := validateDeviceIdentity(controllerID, deviceID); err != nil {
		return DeviceRecord{}, err
	}
	transaction, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return DeviceRecord{}, fmt.Errorf("begin device read: %w", err)
	}
	defer transaction.Rollback()
	revision, err := controllerRevision(ctx, transaction, controllerID)
	if err != nil {
		return DeviceRecord{}, err
	}
	device, err := queryDevice(ctx, transaction, controllerID, deviceID)
	if err != nil {
		return DeviceRecord{}, err
	}
	if err := transaction.Commit(); err != nil {
		return DeviceRecord{}, fmt.Errorf("commit device read: %w", err)
	}
	return DeviceRecord{RegistryRevision: revision, Device: device}, nil
}
