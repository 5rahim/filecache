package filecache

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type testStruct struct {
	Name string
}

func TestCacherSetAndGet(t *testing.T) {

	tempDir := t.TempDir()
	t.Log(tempDir)

	cacher, err := NewCacher(filepath.Join(tempDir))

	bucket := NewBucket("test", 4*time.Second)

	key := "key"
	value := testStruct{
		Name: "value",
	}

	// Add "key" -> value to the bucket, with a TTL of 4 seconds
	err = cacher.Set(bucket.Name(), bucket.TTL(), key, value)
	if err != nil {
		t.Fatalf("Failed to set the value: %v", err)
	}

	var out testStruct
	// Get the value of "key" from the bucket, it shouldn't be expired
	found, err := cacher.Get(bucket.Name(), key, &out)
	if err != nil {
		t.Fatalf("Failed to get the value: %v", err)
	}
	if !found || value.Name != out.Name {
		t.Fatalf("Failed to get the correct value. Expected %v, got %v", value, out)
	}

	time.Sleep(3 * time.Second)

	// Get the value of "key" from the bucket again, it shouldn't be expired
	found, err = cacher.Get(bucket.Name(), key, &out)
	if !found {
		t.Fatalf("Failed to get the value")
	}
	if !found || out != value {
		t.Fatalf("Failed to get the correct value. Expected %v, got %v", value, out)
	}

	// Spin up a goroutine to set "key2" -> value2 to the bucket, with a TTL of 1 second
	// cacher should be thread-safe
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		key2 := "key2"
		value2 := testStruct{
			Name: "value2",
		}
		var out2 testStruct
		err = cacher.Set(bucket.Name(), bucket.TTL(), key2, value2)
		if err != nil {
			t.Errorf("Failed to set the value: %v", err)
			return
		}

		found, err = cacher.Get(bucket.Name(), key2, &out2)
		if err != nil {
			t.Errorf("Failed to get the value: %v", err)
			return
		}

		if !found || value2.Name != out2.Name {
			t.Errorf("Failed to get the correct value. Expected %v, got %v", value2, out2)
			return
		}

		_ = cacher.Delete(bucket.Name(), key2)

	}()

	time.Sleep(2 * time.Second)

	// Get the value of "key" from the bucket, it should be expired
	found, _ = cacher.Get(bucket.Name(), key, &out)
	if found {
		t.Fatalf("Failed to delete the value")
	}

	wg.Wait()

}
