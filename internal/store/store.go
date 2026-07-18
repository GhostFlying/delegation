package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	moderncsqlite "modernc.org/sqlite"
)

const (
	schemaVersion = 2
	busyTimeoutMS = 5000
	walRetryLimit = 8
	sqliteBusy    = 5
)

type Store struct {
	db *sql.DB
}

func Open(ctx context.Context, path string) (*Store, error) {
	resolved, err := preparePath(path)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dataSourceName(resolved))
	if err != nil {
		return nil, fmt.Errorf("open broker state: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(0)

	store := &Store{db: db}
	closeOnError := func(err error) (*Store, error) {
		return nil, errors.Join(err, db.Close())
	}
	if err := db.PingContext(ctx); err != nil {
		return closeOnError(fmt.Errorf("open broker state: %w", err))
	}
	if err := protectDatabaseFile(resolved); err != nil {
		return closeOnError(err)
	}
	if err := store.migrate(ctx); err != nil {
		return closeOnError(err)
	}
	if err := store.enableWAL(ctx); err != nil {
		return closeOnError(err)
	}
	if err := store.checkIntegrity(ctx); err != nil {
		return closeOnError(err)
	}
	return store, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func dataSourceName(path string) string {
	uri := &url.URL{Scheme: "file", Path: sqliteURIPath(path)}
	query := uri.Query()
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeoutMS))
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "synchronous(FULL)")
	uri.RawQuery = query.Encode()
	return uri.String()
}

func preparePath(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", errors.New("broker state path must be absolute")
	}
	path = filepath.Clean(path)
	if err := validateLocalStatePath(path); err != nil {
		return "", err
	}
	directory := filepath.Dir(path)
	createdDirectory := false
	if _, err := os.Lstat(directory); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return "", fmt.Errorf("create broker state directory: %w", err)
		}
		createdDirectory = true
	} else if err != nil {
		return "", fmt.Errorf("inspect broker state directory: %w", err)
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return "", fmt.Errorf("inspect broker state directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("broker state directory must not be a symbolic link")
	}
	if createdDirectory {
		if err := os.Chmod(directory, 0o700); err != nil {
			return "", fmt.Errorf("protect broker state directory: %w", err)
		}
	} else if err := validatePrivateDirectory(info); err != nil {
		return "", err
	}
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("broker state must be a regular file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect broker state: %w", err)
	}
	return path, nil
}

func (s *Store) enableWAL(ctx context.Context) error {
	delay := 10 * time.Millisecond
	for attempt := range walRetryLimit {
		var mode string
		err := s.db.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&mode)
		if err == nil {
			if mode != "wal" {
				return fmt.Errorf("enable broker state WAL: SQLite selected %q", mode)
			}
			return nil
		}
		var sqliteError *moderncsqlite.Error
		if !errors.As(err, &sqliteError) || sqliteError.Code()&0xff != sqliteBusy || attempt == walRetryLimit-1 {
			return fmt.Errorf("enable broker state WAL: %w", err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("enable broker state WAL: %w", ctx.Err())
		case <-timer.C:
		}
		delay *= 2
	}
	panic("unreachable WAL retry loop")
}

func protectDatabaseFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect opened broker state: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("opened broker state is not a regular file")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("protect broker state: %w", err)
	}
	return nil
}

func (s *Store) checkIntegrity(ctx context.Context) error {
	var result string
	if err := s.db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&result); err != nil {
		return fmt.Errorf("check broker state integrity: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("check broker state integrity: %s", result)
	}
	return nil
}

func (s *Store) withImmediateTransaction(ctx context.Context, operation func(*sql.Conn) error) (err error) {
	connection, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("reserve broker state connection: %w", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("begin broker state transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_, _ = connection.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	if err := operation(connection); err != nil {
		return err
	}
	if _, err := connection.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit broker state transaction: %w", err)
	}
	return nil
}
