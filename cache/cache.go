package cache

import (
	"encoding/json"
	"io"
	"sync"
)

func NewCacheManager() *CacheManager {
	cm := &CacheManager{}
	cm.data = make(map[string]string)
	return cm
}

type CacheManager struct {
	data map[string]string
	sync.RWMutex
}

func (cm *CacheManager) Get(key string) (string, bool) {
	cm.RLock()
	defer cm.RUnlock()
	value, found := cm.data[key]
	return value, found
}

func (cm *CacheManager) Put(key, value string) error {
	cm.Lock()
	defer cm.Unlock()
	cm.data[key] = value
	return nil
}

func (cm *CacheManager) Marshal() ([]byte, error) {
	cm.RLock()
	defer cm.RUnlock()
	return json.Marshal(cm.data)
}

func (cm *CacheManager) Unmarshal(serialized io.ReadCloser) error {
	data := make(map[string]string)
	if err := json.NewDecoder(serialized).Decode(&data); err != nil {
		return err
	}
	cm.Lock()
	defer cm.Unlock()
	cm.data = data
	return nil
}
