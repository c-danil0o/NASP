package LRU

import (
	"container/list"
)

type Cache struct {
	cache    *list.List
	size     int
	elements map[string]*list.Element
}

type elementValue struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Tombstone byte
}

type Element struct {
	key   []byte
	value elementValue
}

func CreateCache(size int) *Cache {
	return &Cache{
		size:     size,
		cache:    list.New(),
		elements: make(map[string]*list.Element),
	}
}

func (cache *Cache) Find(key []byte) (*elementValue, bool) {
	element, ok := cache.elements[string(key)]
	if ok {
		cache.cache.MoveToFront(element)
		x := element.Value.(*Element)
		return &x.value, true
	}

	return nil, false
}

func (cache *Cache) Insert(value elementValue) {
	foundElement, ok := cache.elements[string(value.Key)]
	if ok {
		cache.cache.MoveToFront(foundElement)
		return
	}

	if cache.cache.Len() == cache.size {
		eraseElement := cache.cache.Back()
		if eraseElement != nil {
			cache.cache.Remove(eraseElement)
			x := eraseElement.Value.(*Element)
			delete(cache.elements, string(x.value.Key))
		}
	}

	newElement := &Element{key: value.Key, value: value}
	newCache := cache.cache.PushFront(newElement)
	cache.elements[string(value.Key)] = newCache
}
