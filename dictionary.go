package crdt

import (
	"sync"
	"time"
)

type Dictionary struct {
	mutex     sync.RWMutex
	addMap    map[interface{}]*Element
	removeMap map[interface{}]*Element
	size      int
}

func NewDictionary() *Dictionary {
	return &Dictionary{
		addMap:    make(map[interface{}]*Element),
		removeMap: make(map[interface{}]*Element),
		size:      0,
	}
}

// get operation
// - return element if found in addMap, but not in removeMap
// - return nil if not found in addMap, neither in removeMap
// - return nil if not found in addMap, but removeMap
func (d *Dictionary) Get(key interface{}) *Element {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if _, found := d.removeMap[key]; found {
		return nil
	}

	var a *Element
	var found bool
	if a, found = d.addMap[key]; !found {
		return nil
	}
	return a
}

// add operation
// - do nothing if element found in removeMap
// - add to addMap if element not found in addMap
// - update value if element found in addMap and timestamp is after existing one
func (d *Dictionary) Add(key interface{}, value interface{}, timestamp time.Time) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if _, found := d.removeMap[key]; found {
		return
	}

	if a, found := d.addMap[key]; found {
		if a.timestamp.After(timestamp) {
			return
		}
	} else {
		d.size++
	}
	d.addMap[key] = &Element{Value: value, timestamp: timestamp}
}

// update operation
// synonym of add operation
func (d *Dictionary) Update(key interface{}, value interface{}, timestamp time.Time) {
	d.Add(key, value, timestamp)
}

// remove operation
// - do nothing if element not found in addMap
// - add to removeMap if element not found in removeMap
// - update value if element found in removeMap and timestamp is before existing one
func (d *Dictionary) Remove(key interface{}, timestamp time.Time) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	var a *Element
	var found bool
	if a, found = d.addMap[key]; !found {
		return
	}

	if r, found := d.removeMap[key]; found {
		if r.timestamp.Before(timestamp) {
			return
		}
	} else {
		d.size--
	}
	d.removeMap[key] = &Element{Value: a.Value, timestamp: timestamp}
}

func (d *Dictionary) Size() int {
	return d.size
}

func (d1 *Dictionary) Merge(d2 *Dictionary) *Dictionary {
	d1.mutex.RLock()
	d2.mutex.RLock()
	defer d1.mutex.RUnlock()
	defer d2.mutex.RUnlock()

	d3 := NewDictionary()
	for k, v := range d1.addMap {
		d3.Add(k, v.Value, v.timestamp)
	}
	for k, v := range d1.removeMap {
		d3.Remove(k, v.timestamp)
	}
	for k, v := range d2.addMap {
		d3.Add(k, v.Value, v.timestamp)
	}
	for k, v := range d2.removeMap {
		d3.Remove(k, v.timestamp)
	}
	return d3
}
