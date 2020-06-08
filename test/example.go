package main

import (
	"fmt"
	"github.com/allegro/bigcache"
	"github.com/coocood/freecache"
	"log"
	"runtime/debug"
	"time"
)

func main() {
	config := bigcache.Config{
		// number of shards (must be a power of 2)
		Shards: 1024,
		// time after which entry can be evicted
		LifeWindow: 10 * time.Minute,
		// rps * lifeWindow, used only in initial memory allocation
		MaxEntriesInWindow: 1000 * 10 * 60,
		// max entry size in bytes, used only in initial memory allocation
		MaxEntrySize: 500,
		// prints information about additional memory allocation
		Verbose: true,
		// cache will not allocate more memory than this limit, value in MB
		// if value is reached then the oldest entries can be overridden for the new ones
		// 0 value means no size limit
		HardMaxCacheSize: 8192,
		// callback fired when the oldest entry is removed because of its
		// expiration time or no space left for the new entry. Default value is nil which
		// means no callback and it prevents from unwrapping the oldest entry.
		OnRemove: nil,
	}

	cache, initErr := bigcache.NewBigCache(config)
	if initErr != nil {
		log.Fatal(initErr)
	}

	cache.Set("my-unique-key", []byte("value"))

	if entry, err := cache.Get("my-unique-key"); err == nil {
		fmt.Println(string(entry))
	}

	cacheSize := 100 * 1024 * 1024
	fcache := freecache.NewCache(cacheSize)
	debug.SetGCPercent(20)
	key := []byte("abc")
	val := []byte("def")
	expire := 60 // expire in 60 seconds
	fcache.Set(key, val, expire)
	got, err := fcache.Get(key)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(string(got))
	}
	affected := fcache.Del(key)
	fmt.Println("deleted key ", affected)
	fmt.Println("entry count ", fcache.EntryCount())
}
