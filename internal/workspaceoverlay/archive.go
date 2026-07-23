package workspaceoverlay

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/klauspost/compress/zstd"
)

const manifestEntryName = "manifest.json"

var archiveTimestamp = time.Unix(0, 0).UTC()

type PayloadSource func(digest string) (io.ReadCloser, error)

type Extracted struct {
	Manifest     Manifest
	PayloadNames map[string]string
}

func PayloadFileName(digest string) string {
	return "payload-" + digest
}

func WriteArchive(
	ctx context.Context,
	destination io.Writer,
	manifest Manifest,
	openPayload PayloadSource,
) (returnErr error) {
	manifestBytes, err := manifest.CanonicalJSON()
	if err != nil {
		return err
	}
	if int64(len(manifestBytes)) > MaximumManifestBytes {
		return errors.New("workspace overlay manifest exceeds its byte limit")
	}
	encoder, err := zstd.NewWriter(
		destination,
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
		zstd.WithWindowSize(1<<20),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		return fmt.Errorf("create workspace overlay compressor: %w", err)
	}
	tarWriter := tar.NewWriter(encoder)
	defer func() {
		returnErr = errors.Join(returnErr, tarWriter.Close(), encoder.Close())
	}()
	if err := writeEntry(ctx, tarWriter, manifestEntryName, int64(len(manifestBytes)), bytes.NewReader(manifestBytes), ""); err != nil {
		return err
	}
	for _, payload := range manifest.Payloads {
		if err := ctx.Err(); err != nil {
			return err
		}
		reader, err := openPayload(payload.SHA256)
		if err != nil {
			return fmt.Errorf("open workspace overlay payload %s: %w", payload.SHA256, err)
		}
		writeErr := writeEntry(
			ctx, tarWriter, path.Join("payload", payload.SHA256), payload.Size, reader, payload.SHA256,
		)
		closeErr := reader.Close()
		if writeErr != nil || closeErr != nil {
			return errors.Join(writeErr, closeErr)
		}
	}
	return nil
}

func ExtractArchive(
	ctx context.Context,
	source io.Reader,
	destination *os.Root,
) (result Extracted, returnErr error) {
	return extractArchiveWithDecodedLimit(ctx, source, destination, MaximumDecodedBytes)
}

func extractArchiveWithDecodedLimit(
	ctx context.Context,
	source io.Reader,
	destination *os.Root,
	decodedLimit int64,
) (result Extracted, returnErr error) {
	decoder, err := zstd.NewReader(
		source,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxMemory(uint64(MaximumDecodedBytes)),
		zstd.WithDecoderMaxWindow(8*1024*1024),
	)
	if err != nil {
		return Extracted{}, fmt.Errorf("open workspace overlay compressor stream: %w", err)
	}
	defer decoder.Close()
	limited := &hardLimitReader{reader: decoder, remaining: decodedLimit}
	tarReader := tar.NewReader(limited)
	header, err := tarReader.Next()
	if err != nil {
		return Extracted{}, fmt.Errorf("read workspace overlay manifest header: %w", err)
	}
	if err := validateHeader(header, manifestEntryName, header.Size, MaximumManifestBytes); err != nil {
		return Extracted{}, err
	}
	manifestBytes, err := io.ReadAll(tarReader)
	if err != nil {
		return Extracted{}, fmt.Errorf("read workspace overlay manifest: %w", err)
	}
	manifest, err := decodeManifest(manifestBytes)
	if err != nil {
		return Extracted{}, err
	}
	created := make([]string, 0, len(manifest.Payloads))
	defer func() {
		if returnErr == nil {
			return
		}
		for _, name := range created {
			_ = destination.Remove(name)
		}
	}()
	payloadNames := make(map[string]string, len(manifest.Payloads))
	for _, payload := range manifest.Payloads {
		if err := ctx.Err(); err != nil {
			return Extracted{}, err
		}
		header, err := tarReader.Next()
		if err != nil {
			return Extracted{}, fmt.Errorf("read workspace overlay payload header: %w", err)
		}
		entryName := path.Join("payload", payload.SHA256)
		if err := validateHeader(header, entryName, payload.Size, MaximumPayloadBytes); err != nil {
			return Extracted{}, err
		}
		name := PayloadFileName(payload.SHA256)
		file, err := destination.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return Extracted{}, fmt.Errorf("create extracted workspace payload: %w", err)
		}
		created = append(created, name)
		digest := sha256.New()
		written, copyErr := io.Copy(io.MultiWriter(file, digest), &contextReader{ctx: ctx, reader: tarReader})
		syncErr := file.Sync()
		closeErr := file.Close()
		if copyErr != nil || syncErr != nil || closeErr != nil {
			return Extracted{}, errors.Join(copyErr, syncErr, closeErr)
		}
		if written != payload.Size || hex.EncodeToString(digest.Sum(nil)) != payload.SHA256 {
			return Extracted{}, errors.New("workspace overlay payload does not match its descriptor")
		}
		payloadNames[payload.SHA256] = name
	}
	if header, err := tarReader.Next(); !errors.Is(err, io.EOF) {
		if err == nil {
			return Extracted{}, fmt.Errorf("workspace overlay contains unexpected entry %q", header.Name)
		}
		return Extracted{}, fmt.Errorf("finish workspace overlay archive: %w", err)
	}
	var trailing [1]byte
	if count, err := limited.Read(trailing[:]); count != 0 || !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("trailing decoded data")
		}
		return Extracted{}, fmt.Errorf("workspace overlay has data after the tar terminator: %w", err)
	}
	return Extracted{Manifest: manifest, PayloadNames: payloadNames}, nil
}

func writeEntry(
	ctx context.Context,
	w *tar.Writer,
	name string,
	size int64,
	source io.Reader,
	wantDigest string,
) error {
	header := &tar.Header{
		Name: name, Mode: 0o600, Size: size, ModTime: archiveTimestamp,
		Typeflag: tar.TypeReg, Format: tar.FormatUSTAR,
	}
	if err := w.WriteHeader(header); err != nil {
		return fmt.Errorf("write workspace overlay header: %w", err)
	}
	digest := sha256.New()
	limited := io.LimitReader(&contextReader{ctx: ctx, reader: source}, size+1)
	written, err := io.Copy(io.MultiWriter(w, digest), limited)
	if err != nil {
		return fmt.Errorf("write workspace overlay entry: %w", err)
	}
	if written != size {
		return errors.New("workspace overlay payload size changed while it was archived")
	}
	if wantDigest != "" && hex.EncodeToString(digest.Sum(nil)) != wantDigest {
		return errors.New("workspace overlay payload digest changed while it was archived")
	}
	return nil
}

func validateHeader(header *tar.Header, name string, size, maximum int64) error {
	if header.Name != name || header.Typeflag != tar.TypeReg || header.Linkname != "" ||
		header.Mode != 0o600 || header.Size != size || header.Size < 0 || header.Size > maximum ||
		header.Uid != 0 || header.Gid != 0 || header.Uname != "" || header.Gname != "" ||
		header.Devmajor != 0 || header.Devminor != 0 || header.Format != tar.FormatUSTAR ||
		!header.ModTime.Equal(archiveTimestamp) || !header.AccessTime.IsZero() ||
		!header.ChangeTime.IsZero() || len(header.PAXRecords) != 0 || len(header.Xattrs) != 0 {
		return fmt.Errorf("workspace overlay entry %q has invalid metadata", header.Name)
	}
	return nil
}

func decodeManifest(data []byte) (Manifest, error) {
	if int64(len(data)) > MaximumManifestBytes {
		return Manifest{}, errors.New("workspace overlay manifest exceeds its byte limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode workspace overlay manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Manifest{}, errors.New("workspace overlay manifest must contain exactly one JSON value")
	}
	if err := manifest.VerifySemanticHash(); err != nil {
		return Manifest{}, err
	}
	canonical, err := json.Marshal(manifest)
	if err != nil || !bytes.Equal(canonical, data) {
		return Manifest{}, errors.New("workspace overlay manifest is not canonically encoded")
	}
	return manifest, nil
}

type hardLimitReader struct {
	reader    io.Reader
	remaining int64
}

func (r *hardLimitReader) Read(data []byte) (int, error) {
	if r.remaining == 0 {
		return 0, errors.New("workspace overlay decoded data exceeds its byte limit")
	}
	if int64(len(data)) > r.remaining {
		data = data[:r.remaining]
	}
	count, err := r.reader.Read(data)
	r.remaining -= int64(count)
	return count, err
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(data []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(data)
}
