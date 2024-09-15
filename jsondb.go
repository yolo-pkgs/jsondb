package jsondb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/bytedance/sonic"
)

var (
	ErrNotFound = errors.New("not found")
	ErrJSON     = errors.New("json error")
	ErrSync     = errors.New("sync error")
)

type Option int64

const (
	OptSync Option = iota
)

type DB struct {
	opts []Option
	path string
	data map[string]json.RawMessage
	mu   sync.RWMutex
}

func Open(path string, opts ...Option) (*DB, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o755)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create db file: %w", err)
	}

	defer file.Close()

	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute file path: %w", err)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read db file: %w", err)
	}

	if strings.TrimSpace(string(content)) == "" {
		content = []byte("{}")

		_, err := file.Write(content)
		if err != nil {
			return nil, fmt.Errorf("failed write new db: %w", err)
		}
	}

	data := make(map[string]json.RawMessage)
	if err := sonic.Unmarshal(content, &data); err != nil {
		return nil, fmt.Errorf("db content is not valid JSON")
	}

	return &DB{
		opts: opts,
		path: absPath,
		data: data,
		mu:   sync.RWMutex{},
	}, nil
}

func (db *DB) Save() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.sync()
}

func (db *DB) sync() error {
	content, err := json.Marshal(db.data)
	if err != nil {
		return errors.Join(ErrJSON, err)
	}

	if err := os.WriteFile(db.path, content, 0o755); err != nil {
		return errors.Join(ErrSync, err)
	}

	return nil
}

func (db *DB) syncIfNeeded() error {
	if slices.Contains(db.opts, OptSync) {
		if err := db.sync(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Set(key string, val any) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	raw, err := sonic.Marshal(val)
	if err != nil {
		return errors.Join(ErrJSON, err)
	}

	db.data[key] = raw

	return db.syncIfNeeded()
}

func (db *DB) SetRaw(key string, val json.RawMessage) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.data[key] = val

	return db.syncIfNeeded()
}

func (db *DB) Get(key string, val interface{}) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	raw, ok := db.data[key]
	if !ok {
		return ErrNotFound
	}

	if err := sonic.Unmarshal(raw, val); err != nil {
		return errors.Join(ErrJSON, err)
	}

	return nil
}

func (db *DB) GetRaw(key string) (json.RawMessage, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	raw, ok := db.data[key]
	if !ok {
		return nil, ErrNotFound
	}

	return raw, nil
}

func (db *DB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	delete(db.data, key)

	return db.syncIfNeeded()
}

func (db *DB) Iter(fn func(key string, value json.RawMessage) error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for k, v := range db.data {
		if err := fn(k, v); err != nil {
			break
		}
	}
}
