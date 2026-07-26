package resultpackagefiles

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func receivingDirectoryName(packageID, attemptID string) string {
	return ".receiving-" + packageID + "-" + attemptID
}

func openPrivateDirectory(root *os.Root, name string) (*os.Root, error) {
	before, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !privateMode(before.Mode(), true) {
		return nil, errors.New("result package entry is not a private directory")
	}
	directory, err := root.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	openedFile, err := directory.Open(".")
	if err != nil {
		_ = directory.Close()
		return nil, err
	}
	opened, statErr := openedFile.Stat()
	closeErr := openedFile.Close()
	if statErr == nil && (!os.SameFile(before, opened) || !privateMode(opened.Mode(), true)) {
		statErr = errors.New("result package directory changed while it was opened")
	}
	if statErr != nil || closeErr != nil {
		_ = directory.Close()
		return nil, errors.Join(statErr, closeErr)
	}
	return directory, nil
}

func createReceivingDirectory(root *os.Root, inbox store.ResultInbox) error {
	name := receivingDirectoryName(inbox.PackageID, inbox.AttemptID)
	if err := root.Mkdir(name, 0o700); err != nil {
		return err
	}
	directory, err := openPrivateDirectory(root, name)
	if err != nil {
		return err
	}
	defer directory.Close()
	for _, descriptor := range inbox.Manifest.Parts {
		fileName, _ := descriptor.Kind.FileName()
		file, err := directory.OpenFile(fileName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err != nil {
			return fmt.Errorf("create result package part %s: %w", descriptor.Kind, err)
		}
		if err := errors.Join(file.Sync(), file.Close()); err != nil {
			return fmt.Errorf("sync result package part %s: %w", descriptor.Kind, err)
		}
	}
	if err := syncDirectory(directory); err != nil {
		return fmt.Errorf("sync receiving result package: %w", err)
	}
	if err := syncDirectory(root); err != nil {
		return fmt.Errorf("sync result package inbox: %w", err)
	}
	return nil
}

func expectedFiles(manifest protocol.ResultManifest, includeManifest bool) map[string]protocol.ResultPackagePartDescriptor {
	expected := make(map[string]protocol.ResultPackagePartDescriptor, len(manifest.Parts)+1)
	for _, descriptor := range manifest.Parts {
		name, _ := descriptor.Kind.FileName()
		expected[name] = descriptor
	}
	if includeManifest {
		expected[protocol.ResultManifestFileName] = protocol.ResultPackagePartDescriptor{
			Kind: protocol.ResultPackagePartManifest,
		}
	}
	return expected
}

func validateDirectoryEntries(
	directory *os.Root,
	manifest protocol.ResultManifest,
	includeManifest, allowManifest bool,
) error {
	expected := expectedFiles(manifest, includeManifest)
	entries, err := fs.ReadDir(directory.FS(), ".")
	if err != nil {
		return fmt.Errorf("list result package directory: %w", err)
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if name == protocol.ResultManifestFileName && allowManifest {
			seen[name] = struct{}{}
			continue
		}
		if _, ok := expected[name]; !ok {
			return fmt.Errorf("unexpected result package entry %q", name)
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("duplicate result package entry %q", name)
		}
		seen[name] = struct{}{}
	}
	for name := range expected {
		if _, ok := seen[name]; !ok {
			return fmt.Errorf("result package entry %q is missing", name)
		}
	}
	return nil
}

func openRegularFile(directory *os.Root, name string) (*os.File, os.FileInfo, error) {
	before, err := directory.Lstat(name)
	if err != nil {
		return nil, nil, err
	}
	if !before.Mode().IsRegular() || !privateMode(before.Mode(), false) {
		return nil, nil, errors.New("result package entry is not a private regular file")
	}
	file, err := directory.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return nil, nil, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) || !privateMode(opened.Mode(), false) {
		_ = file.Close()
		if err == nil {
			err = errors.New("result package file changed while it was opened")
		}
		return nil, nil, err
	}
	return file, opened, nil
}

func verifyManifestFile(directory *os.Root, metadata protocol.ResultPackageMetadata) error {
	file, info, err := openRegularFile(directory, protocol.ResultManifestFileName)
	if err != nil {
		return err
	}
	defer file.Close()
	if info.Size() != metadata.ManifestDescriptor.Size {
		return errors.New("result manifest file size does not match its descriptor")
	}
	data, err := io.ReadAll(io.LimitReader(file, protocol.MaximumResultManifestBytes+1))
	if err != nil {
		return fmt.Errorf("read result manifest file: %w", err)
	}
	if !slices.Equal(data, metadata.Manifest) {
		return errors.New("result manifest file does not match authoritative metadata")
	}
	return nil
}

func writeManifestFile(directory *os.Root, metadata protocol.ResultPackageMetadata) error {
	file, err := directory.OpenFile(
		protocol.ResultManifestFileName,
		os.O_CREATE|os.O_EXCL|os.O_RDWR,
		0o600,
	)
	if errors.Is(err, fs.ErrExist) {
		var info os.FileInfo
		file, info, err = openRegularFile(directory, protocol.ResultManifestFileName)
		if err != nil {
			return err
		}
		matches := info.Size() == metadata.ManifestDescriptor.Size
		if matches {
			data, readErr := io.ReadAll(io.LimitReader(file, protocol.MaximumResultManifestBytes+1))
			if readErr != nil {
				return errors.Join(fmt.Errorf("read existing result manifest: %w", readErr), file.Close())
			}
			matches = slices.Equal(data, metadata.Manifest)
		}
		if matches {
			return errors.Join(file.Sync(), file.Close())
		}
		if err := file.Truncate(0); err != nil {
			return errors.Join(fmt.Errorf("truncate partial result manifest: %w", err), file.Close())
		}
	}
	if err != nil {
		return fmt.Errorf("create result manifest file: %w", err)
	}
	written, writeErr := file.WriteAt(metadata.Manifest, 0)
	if writeErr == nil && written != len(metadata.Manifest) {
		writeErr = io.ErrShortWrite
	}
	return errors.Join(writeErr, file.Sync(), file.Close())
}

func hashRegularFile(ctx context.Context, file *os.File, expectedSize int64) (string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	digest := sha256.New()
	reader := &contextReader{ctx: ctx, source: io.LimitReader(file, expectedSize+1)}
	written, err := io.CopyBuffer(digest, reader, make([]byte, 128*1024))
	if err != nil {
		return "", err
	}
	if written != expectedSize {
		return "", errors.New("result package file size changed during verification")
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func removePackageDirectory(root *os.Root, name string) error {
	_, err := root.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	directory, err := openPrivateDirectory(root, name)
	if err != nil {
		return err
	}
	if err := directory.Close(); err != nil {
		return err
	}
	if err := root.RemoveAll(name); err != nil {
		return err
	}
	return syncDirectory(root)
}

type contextReader struct {
	ctx    context.Context
	source io.Reader
}

func (r *contextReader) Read(destination []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.source.Read(destination)
}
