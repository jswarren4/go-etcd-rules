package mirror

import (
	"sync"
	"time"
)

// Source is the repository to be mirrored.
type Source interface {
	Get(key string) (*string, error)
}

type repo struct {
	// Local copy of a subset of the source repo's data.
	kv map[string]string
	// Mutex to control concurrent read/write access to the
	// repo
	mutex sync.RWMutex
	// These are keys that have nil values mapped to the last
	// time the key was read which is used to eventually remove
	// unneeded values.
	nils map[string]time.Time
	// How long nil values should be tracked since the last time
	// they were read.
	nilExpiration time.Duration
	// Mutex for tracking nil values
	nilMutex sync.RWMutex
	// The source repo
	src Source
	// Current time
	now func() time.Time
	// Nil query count; used to determine when to clear out stale nil
	// values
	nilQueries int
	// Nil query refresh interval; how many nil queries to perform before
	// doing a refresh
	nilQueryInterval int
}

func (r *repo) Get(key string, refresh bool) (*string, error) {
	if !refresh {
		r.nilMutex.Lock()
		defer r.nilMutex.Unlock()
		r.nilQueries++
		if r.nilQueries >= r.nilQueryInterval {
			r.nilQueries = 0
			// Clear out stale entries and look for
			// value being queried at the same time.
			now := r.now()
			found := false
			for nilKey := range r.nils {
				// This is actually the key being queried.
				if nilKey == key {
					found = true
					// Update read time for nil value.
					r.nils[nilKey] = now
					continue
				}
				// Expired?
				if now.Sub(r.nils[nilKey]) > r.nilExpiration {
					delete(r.nils, nilKey)
				}
			}
			if found {
				return nil, nil
			}
		} else {
			if _, ok := r.nils[key]; ok {
				r.nils[key] = r.now()
				return nil, nil
			}
		}
		val := r.getLocal(key)
		if val == nil {
			r.nils[key] = r.now()
		}
		return val, nil
	}
	val, err := r.src.Get(key)
	if err == nil {
		if val == nil {
			r.nilMutex.Lock()
			defer r.nilMutex.Unlock()
			r.nils[key] = r.now()
			return nil, nil
		}
		r.mutex.Lock()
		defer r.mutex.Unlock()
		r.kv[key] = *val
	}
	return val, err
}
func (r *repo) getLocal(key string) *string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	if val, ok := r.kv[key]; ok {
		return &val
	}
	return nil
}

func (r *repo) Update(key string, value *string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if value == nil {
		delete(r.kv, key)
		r.nilMutex.Lock()
		defer r.nilMutex.Unlock()
		r.nils[key] = r.now()
		return
	}
	r.kv[key] = *value
}

func (r *repo) GetAll() map[string]*string {
	out := map[string]*string{}
	func() {
		r.mutex.RLock()
		defer r.mutex.RUnlock()
		for k, v := range r.kv {
			func(k, v string) {
				out[k] = &v
			}(k, v)
		}
	}()
	r.nilMutex.RLock()
	defer r.nilMutex.RUnlock()
	// Do not update nil read timestamps; otherwise no value will
	// ever be deleted.
	for k := range r.nils {
		out[k] = nil
	}
	return out
}
