use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::rc::Rc;
use log::{debug, warn};

struct Entry<V> {
    use_count: usize,
    rc: Rc<V>
}

/**
 * A cache that tracks which items in it have been used the most, and avoids
 * evicting those ones.
 *
 * Values are added and borrowed wrapped in Rc: eviction from the cache
 * will not interfere with current users of an item.
 */
pub struct Cache<K, V> {
    entries: HashMap<K, Entry<V>>,
    max_entries: usize,
    evictables: Vec<K>
}

impl<K, V> Cache<K, V>
where K: Hash + Eq + Clone + Debug, V: Sized {
    pub fn new(max_entries: usize) -> Cache<K, V> {
        Cache { entries: HashMap::new(), max_entries, evictables: Vec::new() }
    }

    /**
     * Add a new value to the cache with the associated key.
     *
     * Both key and value are consumed by this function and will then belong
     * to the cache.
     */
    pub fn add(&mut self, key: K, rc: Rc<V>) {
        self.check_capacity();
        debug!("Key {key:?} added");
        self.entries.insert(key, Entry { use_count: 1, rc });
    }

    pub fn get(&mut self, key: &K) -> Option<Rc<V>> {
        let mut entry = self.entries.get_mut(key)?;
        entry.use_count += 1;
        Some(entry.rc.clone())
    }

    /**
     * Evict an item from the cache.  The result is `true` if the item
     * was successfully evicted, or `false` if the item was pinned or there
     * there was nothing under that key.
     */
    pub fn evict(&mut self, key: &K) -> bool {
        let item = self.entries.get(key);
        let Some(entry) = item else { return false; };

        if Rc::strong_count(&entry.rc) > 1 {
            debug!("Key {key:?} not evicted as it is in use");
            return false;
        }

        debug!("Key {key:?} evicted");
        self.entries.remove(key);
        true
    }

    pub fn check_capacity(&mut self) {
        const MAX_FIND_ATTEMPTS: usize = 10;
        let mut find_attempts = 0;
        while self.entries.len() >= self.max_entries {

            /* If there are no evictable items, go find some. */
            if self.evictables.is_empty() {

               /* Since it's possible to fail to evict anything from the cache (if every single
                  thing is in use somewhere else), we only try up to MAX_FIND_ATTEMPTS, after
                  which we just clear the entire cache. */
                if find_attempts >= MAX_FIND_ATTEMPTS {
                    warn!("Too many attempts to find evictables, forcibly emptying cache");
                    self.entries.clear();
                    return;
                } else {
                    find_attempts += 1;
                }

                /* For each item, if its current use_count is zero, add it to the evictables queue.
                   Otherwise decrement the use_count. */
                for (key, entry) in self.entries.iter_mut() {
                    if entry.use_count == 0 {
                        self.evictables.push(key.clone());
                    } else {
                        entry.use_count -= 1;
                    }
                }
            }

            /* Try evicting evictables until the cache isn't overflowing. */
            if let Some(key) = self.evictables.pop() {
                self.evict(&key);
            }
        }
    }
}

#[cfg(test)]
mod cache_tests {
    use super::*;

    #[test]
    fn missing_key() {
        let mut cache: Cache<u32, u32> = Cache::new(100);

        assert!(cache.get(&5).is_none());
    }

    #[test]
    fn borrow_and_return() {
        let mut cache: Cache<u32, u32> = Cache::new(100);
        cache.add(5, Rc::new(42));

        let item = cache.get(&5);

        assert_eq!(Rc::strong_count(&cache.entries.get(&5).unwrap().rc), 2);

        drop(item);

        assert_eq!(Rc::strong_count(&cache.entries.get(&5).unwrap().rc), 1);
    }

    #[test]
    fn borrow_and_use() {
        let mut cache: Cache<u32, u32> = Cache::new(100);
        cache.add(5, Rc::new(42));

        let item = cache.get(&5);

        assert!(item.is_some());

        let rc = item.unwrap();

        let value = *rc;
        assert_eq!(value, 42);
    }

    #[test]
    fn borrow_and_then_add_more() {
        let mut cache: Cache<u32, u32> = Cache::new(100);
        cache.add(5, Rc::new(42));

        let item = cache.get(&5);

        for i in 100..1000 {
            cache.add(i, Rc::new(i));
        }

        let rc = item.unwrap();

        let value = *rc;
        assert_eq!(value, 42);
    }

    #[test]
    fn borrow_two() {
        let mut cache: Cache<u32, u32> = Cache::new(100);
        cache.add(5, Rc::new(42));
        cache.add(7, Rc::new(99));

        let item = cache.get(&5);
        let item2 = cache.get(&7);

        let rc = item.unwrap();
        let rc2 = item2.unwrap();

        let value = *rc;
        let value2 = *rc2;

        assert_eq!(value, 42);
        assert_eq!(value2, 99);
    }

    #[test]
    fn borrow_same_one_twice() {
        let mut cache: Cache<u32, u32> = Cache::new(100);
        cache.add(5, Rc::new(42));

        let item = cache.get(&5);
        let item2 = cache.get(&5);

        let rc = item.unwrap();
        let rc2 = item2.unwrap();

        let value = *rc;
        let value2 = *rc2;

        assert_eq!(value, 42);
        assert_eq!(value2, 42);

        assert_eq!(Rc::strong_count(&cache.entries.get(&5).unwrap().rc), 3);

        drop(rc);
        drop(rc2);

        assert_eq!(Rc::strong_count(&cache.entries.get(&5).unwrap().rc), 1);
    }
}

#[cfg(test)]
mod eviction_tests {
    use super::*;

    #[test]
    fn evict_nothing_borrowed() {
        let mut cache: Cache<u32, u32> = Cache::new(100);
        cache.add(5, Rc::new(42));

        assert_eq!(cache.evict(&5), true);

        assert_eq!(cache.entries.len(), 0);
    }

    #[test]
    fn evict_something_not_there() {
        let mut cache: Cache<u32, u32> = Cache::new(100);

        assert_eq!(cache.evict(&5), false);
    }

    #[test]
    fn try_evict_something_borrowed() {
        let mut cache: Cache<u32, u32> = Cache::new(100);
        cache.add(5, Rc::new(42));

        let item = cache.get(&5);

        assert_eq!(cache.evict(&5), false);

        assert_eq!(cache.entries.len(), 1);
    }
}
