package lytecache

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// lockKeyPrefix namespaces distributed-lock keys away from ordinary
// user keys within the same cache namespace.
const lockKeyPrefix = "__lock__:"

// lockPollInterval is how often Lock retries acquisition while waiting.
const lockPollInterval = 50 * time.Millisecond

// Lock is a process-safe distributed lock obtained from [Cache.Lock].
type Lock struct {
	cache *Cache
	name  string
	token string
}

// Lock attempts to acquire a process-safe lock named name, polling roughly
// every 50ms until timeout elapses. It is built on the same atomic Add
// (Redis "SET NX") semantics as everything else, so only one holder --
// across goroutines *and* processes sharing the cache file -- can hold a
// given lock name at once.
//
// The lock is also given timeout as its own TTL, as a safety net: if the
// holder's process crashes before calling Release, the lock still expires
// on its own instead of wedging forever.
func (c *Cache) Lock(name string, timeout time.Duration) (*Lock, error) {
	token, err := randomToken()
	if err != nil {
		return nil, fmt.Errorf("lytecache: lock %q: %w", name, err)
	}
	lockKey := lockKeyPrefix + name
	deadline := time.Now().Add(timeout)

	for {
		ok, err := c.add(lockKey, token, false, TTL(timeout))
		if err != nil {
			return nil, fmt.Errorf("lytecache: lock %q: %w", name, err)
		}
		if ok {
			return &Lock{cache: c, name: name, token: token}, nil
		}
		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf("%w: could not acquire lock %q within %s", ErrLockTimeout, name, timeout)
		}
		time.Sleep(lockPollInterval)
	}
}

// Release releases the lock. It only deletes the underlying row if this
// Lock's token still matches what is stored -- guarding against releasing
// a lock that expired and was subsequently acquired by someone else.
func (l *Lock) Release() error {
	lockKey := lockKeyPrefix + l.name
	data, _, err := encodeValue(l.token)
	if err != nil {
		return fmt.Errorf("lytecache: release lock %q: %w", l.name, err)
	}
	_, err = l.cache.writeDB.Exec(
		`DELETE FROM cache WHERE namespace = ? AND key = ? AND value = ?`,
		l.cache.namespace, lockKey, data)
	if err != nil {
		return fmt.Errorf("lytecache: release lock %q: %w", l.name, err)
	}
	return nil
}

func randomToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
