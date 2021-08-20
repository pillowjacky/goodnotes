package crdt_test

import (
	"fmt"
	"testing"
	"time"

	. "github.com/pillowjacky/goodnotes-interview"
	"github.com/stretchr/testify/assert"
)

// test if add / update / remove works properly
func TestOperation(t *testing.T) {
	d := NewDictionary()
	d.Add("k1", "v1", time.Now())
	d.Add("k2", "v", time.Now())
	d.Update("k2", "v2", time.Now())
	d.Add("k3", "v3", time.Now())
	d.Remove("k3", time.Now())

	assert.Equal(t, "v1", d.Get("k1").Value)
	assert.Equal(t, "v2", d.Get("k2").Value)
	assert.Nil(t, d.Get("k3"))
	assert.Nil(t, d.Get("k4"))
}

// test if add / update / remove works properly with concurrent access
func TestConcurrentOperation(t *testing.T) {
	d := NewDictionary()

	for i := 1; i <= 1000; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		now := time.Now()
		go d.Add(k, v, now)
		go d.Update(k, v, now)
		time.Sleep(time.Millisecond)
	}

	assert.Equal(t, 1000, d.Size())
	assert.Equal(t, "v1", d.Get("k1").Value)
	assert.Equal(t, "v2", d.Get("k2").Value)

	for i := 1; i <= 1000; i++ {
		k := fmt.Sprintf("k%d", i)
		now := time.Now()
		go d.Remove(k, now)
		time.Sleep(time.Millisecond)
	}

	assert.Equal(t, 0, d.Size())
	assert.Nil(t, d.Get("k1"))
	assert.Nil(t, d.Get("k2"))
}

// test if merge works properly
func TestMerge(t *testing.T) {
	d1 := NewDictionary()
	d1.Add("k1", "v1", time.Now())
	d1.Add("k2", "v2", time.Now())
	d1.Remove("k2", time.Now())

	d2 := NewDictionary()
	d2.Add("k3", "v3", time.Now())
	d2.Add("k4", "v4", time.Now())
	d2.Remove("k4", time.Now())

	d3 := d1.Merge(d2)
	assert.Equal(t, "v1", d3.Get("k1").Value)
	assert.Nil(t, d3.Get("k2"))
	assert.Equal(t, "v3", d3.Get("k3").Value)
	assert.Nil(t, d3.Get("k4"))
}
