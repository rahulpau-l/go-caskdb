package caskdb

type MemoryStore struct {
	data map[string]KeyEntry
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{make(map[string]KeyEntry)}
}

func (m *MemoryStore) Get(key string) KeyEntry {
	return m.data[key]
}

func (m *MemoryStore) Set(key string, value KeyEntry) {
	m.data[key] = value
}

func (m *MemoryStore) Close() bool {
	return true
}
