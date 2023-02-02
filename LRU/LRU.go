package LRU

import (
	"container/list"
	container "github.com/c-danil0o/NASP/DataContainer"
)

type Cache struct {
	cache    *list.List
	size     int
	elements map[string]*list.Element
}

type elementValue struct {
	key       []byte
	value     []byte
	timestamp int64
	tombstone byte
}

func (element *elementValue) Value() []byte {
	return element.value
}
func (element *elementValue) Key() []byte {
	return element.key
}
func (element *elementValue) Tombstone() byte {
	return element.tombstone
}
func (element *elementValue) Timestamp() int64 {
	return element.timestamp
}
func (element *elementValue) SetKey(key []byte) {
	element.key = key
}
func (element *elementValue) SetValue(value []byte) {
	element.value = value
}
func (element *elementValue) SetTimestamp(timestamp int64) {
	element.timestamp = timestamp
}
func (element *elementValue) SetTombstone(tombstone byte) {
	element.tombstone = tombstone
}

type Element struct {
	key   []byte
	value container.DataNode
}

func CreateCache(size int) *Cache {
	return &Cache{
		size:     size,
		cache:    list.New(),
		elements: make(map[string]*list.Element),
	}
}

func (cache *Cache) Find(key []byte) (container.DataNode, bool) {
	element, ok := cache.elements[string(key)]
	if ok {
		cache.cache.MoveToFront(element)
		x := element.Value.(*Element)
		return x.value, true
	}

	return nil, false
}

func (cache *Cache) Insert(value container.DataNode) {
	foundElement, ok := cache.elements[string(value.Key())]
	if ok {
		cache.cache.MoveToFront(foundElement)
		return
	}

	if cache.cache.Len() == cache.size {
		eraseElement := cache.cache.Back()
		if eraseElement != nil {
			cache.cache.Remove(eraseElement)
			x := eraseElement.Value.(*Element)
			delete(cache.elements, string(x.value.Key()))
		}
	}

	newElement := &Element{key: value.Key(), value: value}
	newCache := cache.cache.PushFront(newElement)
	cache.elements[string(value.Key())] = newCache
}
