package filecache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// CacheStore represents a single-process, file-based, key/value cache store.
type CacheStore struct {
	filePath string
	mu       sync.Mutex
	data     map[string]*cacheItem
}

// Bucket represents a cache bucket with a name and TTL.
type Bucket struct {
	name string
	ttl  time.Duration
}

var Ext = ".cache"

func NewBucket(name string, ttl time.Duration) Bucket {
	return Bucket{name: name, ttl: ttl}
}

func (b *Bucket) Name() string {
	return b.name
}

func (b *Bucket) TTL() time.Duration {
	return b.ttl
}

type Cacher struct {
	dir    string
	stores map[string]*CacheStore
	mu     sync.Mutex
	ext    string
}

type cacheItem struct {
	Value      interface{} `json:"value"`
	Expiration *time.Time  `json:"expiration,omitempty"`
}

func NewCacher(dir string) (*Cacher, error) {
	_ = os.MkdirAll(dir, os.ModePerm)
	return &Cacher{
		stores: make(map[string]*CacheStore),
		dir:    dir,
		ext:    Ext,
	}, nil
}

func NewCacherWithExt(dir, ext string) (*Cacher, error) {
	_ = os.MkdirAll(dir, os.ModePerm)
	return &Cacher{
		stores: make(map[string]*CacheStore),
		dir:    dir,
		ext:    ext,
	}, nil
}

// Close closes all the cache stores.
func (c *Cacher) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, store := range c.stores {
		if err := store.saveToFile(); err != nil {
			return err
		}
	}
	return nil
}

// getStore returns a cache store for the given bucket name and TTL.
func (c *Cacher) getStore(name string) (*CacheStore, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	store, ok := c.stores[name]
	if !ok {
		store = &CacheStore{
			filePath: filepath.Join(c.dir, name+".cache"),
			data:     make(map[string]*cacheItem),
		}
		if err := store.loadFromFile(); err != nil {
			return nil, err
		}
		c.stores[name] = store
	}
	return store, nil
}

// Set sets the value for the given key in the given bucket.
func (c *Cacher) Set(bucketName string, ttl time.Duration, key string, value interface{}) error {
	store, err := c.getStore(bucketName)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.data[key] = &cacheItem{Value: value, Expiration: ToPtr(time.Now().Add(ttl))}
	return store.saveToFile()
}

func Range[T any](c *Cacher, bucketName string, f func(key string, value T) bool) error {
	store, err := c.getStore(bucketName)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	for key, item := range store.data {
		if item.Expiration != nil && time.Now().After(*item.Expiration) {
			delete(store.data, key)
		} else {
			itemVal, err := json.Marshal(item.Value)
			if err != nil {
				return err
			}
			var out T
			err = json.Unmarshal(itemVal, &out)
			if err != nil {
				return err
			}
			if !f(key, out) {
				break
			}
		}
	}

	return store.saveToFile()
}

// Get retrieves the value for the given key from the given bucket.
// If the key does not exist or has expired, it returns false.
// This removes the item from the cache if it has expired.
func (c *Cacher) Get(bucketName string, key string, out interface{}) (bool, error) {
	store, err := c.getStore(bucketName)
	if err != nil {
		return false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	item, ok := store.data[key]
	if !ok {
		return false, nil
	}
	if item.Expiration != nil && time.Now().After(*item.Expiration) {
		delete(store.data, key)
		_ = store.saveToFile() // Ignore errors here
		return false, nil
	}
	data, err := json.Marshal(item.Value)
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal(data, out)
}

func GetAll[T any](c *Cacher, bucketName string) (map[string]T, error) {
	store, err := c.getStore(bucketName)
	if err != nil {
		return nil, err
	}

	data := make(map[string]T)
	err = Range(c, bucketName, func(key string, value T) bool {
		data[key] = value
		return true
	})
	if err != nil {
		return nil, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	return data, store.saveToFile()
}

// Delete deletes the value for the given key from the given bucket.
func (c *Cacher) Delete(bucketName string, key string) error {
	store, err := c.getStore(bucketName)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.data, key)
	return store.saveToFile()
}

func DeleteIf[T any](c *Cacher, bucketName string, cond func(key string, value T) bool) error {
	store, err := c.getStore(bucketName)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	for key, item := range store.data {
		itemVal, err := json.Marshal(item.Value)
		if err != nil {
			return err
		}
		var out T
		err = json.Unmarshal(itemVal, &out)
		if err != nil {
			return err
		}
		if cond(key, out) {
			delete(store.data, key)
		}
	}

	return store.saveToFile()
}

// EmptyBucket empties the given bucket (removes all items).
func (c *Cacher) EmptyBucket(bucketName string) error {
	store, err := c.getStore(bucketName)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.data = make(map[string]*cacheItem)
	return store.saveToFile()
}

// RemoveBucket removes the given bucket.
func (c *Cacher) RemoveBucket(bucketName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.stores[bucketName]; ok {
		delete(c.stores, bucketName)
	}
	_ = os.Remove(filepath.Join(c.dir, bucketName+Ext))
	return nil
}

func (c *Cacher) CleanBucket(bucketName string) error {
	store, err := c.getStore(bucketName)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	for key, item := range store.data {
		if item.Expiration != nil && time.Now().After(*item.Expiration) {
			delete(store.data, key)
		}
	}

	return store.saveToFile()
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// SetFrozen sets the item with no expiration in the given bucket.
func (c *Cacher) SetFrozen(bucketName string, key string, value interface{}) error {
	store, err := c.getStore(bucketName)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.data[key] = &cacheItem{Value: value, Expiration: nil} // No expiration
	return store.saveToFile()
}

// GetFrozen retrieves the value for the given key from the given bucket without checking for expiration.
func (c *Cacher) GetFrozen(bucketName string, key string, out interface{}) (bool, error) {
	store, err := c.getStore(bucketName)
	if err != nil {
		return false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	item, ok := store.data[key]
	if !ok {
		return false, nil
	}
	data, err := json.Marshal(item.Value)
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal(data, out)
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (cs *CacheStore) loadFromFile() error {
	file, err := os.Open(cs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // File does not exist, so nothing to load
		}
		return fmt.Errorf("filecache: failed to open cache file: %w", err)
	}
	defer file.Close()

	if err := json.NewDecoder(file).Decode(&cs.data); err != nil {
		return fmt.Errorf("filecache: failed to decode cache data: %w", err)
	}
	return nil
}

func (cs *CacheStore) saveToFile() error {
	file, err := os.Create(cs.filePath)
	if err != nil {
		return fmt.Errorf("filecache: failed to create cache file: %w", err)
	}
	defer file.Close()

	if err := json.NewEncoder(file).Encode(cs.data); err != nil {
		return fmt.Errorf("filecache: failed to encode cache data: %w", err)
	}
	return nil
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RemoveAllBy removes all files in the cache directory that match the given filter.
func (c *Cacher) RemoveAllBy(filter func(filename string) bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := filepath.Walk(c.dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			if !strings.HasSuffix(info.Name(), Ext) {
				return nil
			}
			if filter(info.Name()) {
				if err := os.Remove(filepath.Join(c.dir, info.Name())); err != nil {
					return fmt.Errorf("filecache: failed to remove file: %w", err)
				}
			}
		}
		return nil
	})

	c.stores = make(map[string]*CacheStore)
	return err
}

// GetTotalSize returns the total size of all files in the cache directory that match the given filter.
// The size is in bytes.
func (c *Cacher) GetTotalSize() (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var totalSize int64
	err := filepath.Walk(c.dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("filecache: failed to walk the cache directory: %w", err)
	}

	return totalSize, nil
}
