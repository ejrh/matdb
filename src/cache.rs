use std::collections::HashMap;
use std::hash::Hash;

/**
 * A cache that tracks which items in it are currently being used.
 *
 * When an item is borrowed, its pin count is incremented.  A non-zero
 * pin count indicates that the item is currently in use, and should not be
 * evicted from the cache.
 */
pub struct Cache<K, V> {
    entries: HashMap<K, Box<CacheItem<V>>>
}

/**
 * The item inside the cache.
 */
pub struct CacheItem<V> {
    pin_count: u32,
    use_count: u32,
    value: V,
}

/**
 * A borrowed item, or rather a handle to it.
 *
 * The actual item can be got with the `get` method.
 *
 * When this is dropped (explicitly or implicitly), the pin count
 * on the corresponding item is decremented.
 */
pub struct BorrowedCacheItem<V> {
    item: *mut CacheItem<V>
}

impl<K, V> Cache<K, V>
where K: Hash + Eq {
    pub fn new() -> Cache<K, V> {
        Cache { entries: HashMap::new() }
    }

    /**
     * Add a new value to the cache with the associated key.
     *
     * Both key and value are consumed by this function and will then belong
     * to the cache.
     */
    pub fn add(&mut self, key: K, value: V) {
        let item = CacheItem {
            pin_count: 0,
            use_count: 1,
            value
        };
        self.entries.insert(key, Box::new(item));
    }

    /**
     * Borrow a item from the cache by key.  The result is `None` if
     * there is nothing under that key in the cache, otherwise it is
     * `Some(borrowed)` where `borrowed` is a handle to the item.
     */
    pub fn borrow(&mut self, key: &K) -> Option<BorrowedCacheItem<V>> {
        let item = self.entries.get_mut(key)?.as_mut();
        item.use_count += 1;
        item.pin_count += 1;
        Some(BorrowedCacheItem {
            item
        })
    }

    /**
     * Evict an item from the cache.  The result is `true` if the item
     * was successfully evicted, or `false` if the item was pinned or there
     * there was nothing under that key.
     */
    pub fn evict(&mut self, key: &K) -> bool {
        let item = self.entries.get(key);

        if item.is_none() { return false; }

        let item = item.unwrap();

        if item.pin_count > 0 { return false; }

        self.entries.remove(key);
        true
    }
}

impl<V> BorrowedCacheItem<V> {
    pub fn get(&self) -> &V {
        let item = unsafe { self.item.as_ref().expect("should point to an item") };
        &(item.value)
    }
}

impl<V> Drop for BorrowedCacheItem<V> {
    fn drop(&mut self) {
        let item = unsafe { self.item.as_mut().expect("should point to an item") };
        item.pin_count -= 1;
    }
}

#[cfg(test)]
mod cache_tests {
    use crate::cache::Cache;

    #[test]
    fn missing_key() {
        let mut cache: Cache<u32, u32> = Cache::new();

        assert!(cache.borrow(&5).is_none());
    }

    #[test]
    fn borrow_and_return() {
        let mut cache: Cache<u32, u32> = Cache::new();
        cache.add(5, 42);

        let item = cache.borrow(&5);

        assert_eq!(cache.entries.get(&5).unwrap().pin_count, 1);

        drop(item);

        assert_eq!(cache.entries.get(&5).unwrap().pin_count, 0);
    }


    #[test]
    fn borrow_and_use() {
        let mut cache: Cache<u32, u32> = Cache::new();
        cache.add(5, 42);

        let item = cache.borrow(&5);

        assert!(item.is_some());

        let item = item.unwrap();

        let value = item.get();
        assert_eq!(value, &42);
    }

    #[test]
    fn borrow_and_then_add_more() {
        let mut cache: Cache<u32, u32> = Cache::new();
        cache.add(5, 42);

        let item = cache.borrow(&5);

        for i in 100..1000 {
            cache.add(i, i);
        }

        let item = item.unwrap();

        let value = item.get();
        assert_eq!(value, &42);
    }

    #[test]
    fn borrow_two() {
        let mut cache: Cache<u32, u32> = Cache::new();
        cache.add(5, 42);
        cache.add(7, 99);

        let item = cache.borrow(&5);
        let item2 = cache.borrow(&7);

        let item = item.unwrap();
        let item2 = item2.unwrap();

        let value = item.get();
        let value2 = item2.get();

        assert_eq!(value, &42);
        assert_eq!(value2, &99);
    }

    #[test]
    fn borrow_same_one_twice() {
        let mut cache: Cache<u32, u32> = Cache::new();
        cache.add(5, 42);

        let item = cache.borrow(&5);
        let item2 = cache.borrow(&5);

        let item = item.unwrap();
        let item2 = item2.unwrap();

        let value = item.get();
        let value2 = item2.get();

        assert_eq!(value, &42);
        assert_eq!(value2, &42);

        assert_eq!(cache.entries.get(&5).unwrap().pin_count, 2);

        drop(item);
        drop(item2);

        assert_eq!(cache.entries.get(&5).unwrap().pin_count, 0);
    }
}

#[cfg(test)]
mod eviction_tests {
    use crate::cache::Cache;

    #[test]
    fn evict_nothing_borrowed() {
        let mut cache: Cache<u32, u32> = Cache::new();
        cache.add(5, 42);

        assert_eq!(cache.evict(&5), true);

        assert_eq!(cache.entries.len(), 0);
    }

    #[test]
    fn evict_something_not_there() {
        let mut cache: Cache<u32, u32> = Cache::new();

        assert_eq!(cache.evict(&5), false);
    }

    #[test]
    fn try_evict_something_borrowed() {
        let mut cache: Cache<u32, u32> = Cache::new();
        cache.add(5, 42);

        let item = cache.borrow(&5);

        assert_eq!(cache.evict(&5), false);

        assert_eq!(cache.entries.len(), 1);

        drop(item);
    }
}
