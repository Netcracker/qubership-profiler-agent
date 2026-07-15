package query

import (
	"context"
	"sync"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
)

// dictCacheCap bounds each per-pod-restart dictionary cache. Eviction is
// arbitrary (map iteration order): the caches are a revalidation shortcut,
// not a correctness surface.
const dictCacheCap = 512

type (
	// dictCache keeps the per-pod-restart dictionaries the hot /tree path
	// resolves against (02 §2.6): live dictionaries revalidate against the
	// hosting replica with their ETag (a dictionary only grows, so a 304 is
	// the common case). Closed pod-restarts need no cache — a sealed row
	// carries its own dictionary subset (dict_words_json, №3, №23).
	dictCache struct {
		mu  sync.Mutex
		hot map[model.PodTuple]hotDictEntry
	}

	hotDictEntry struct {
		etag  string
		words []string
	}
)

func newDictCache() *dictCache {
	return &dictCache{hot: map[model.PodTuple]hotDictEntry{}}
}

// hotDictionary resolves a live pod-restart's dictionary through the replica
// that just served its blob, revalidating the cached copy by ETag. A replica
// error falls back to the cached words when there are any — the dictionary is
// append-only, so a stale copy only renders the newest ids as placeholders.
func (s *Service) hotDictionary(ctx context.Context, baseURL string, tuple model.PodTuple) ([]string, error) {
	s.dicts.mu.Lock()
	cached, hasCached := s.dicts.hot[tuple]
	s.dicts.mu.Unlock()

	etag := ""
	if hasCached {
		etag = cached.etag
	}
	dict, notModified, found, err := s.hot.FetchDictionary(ctx, baseURL, tuple, etag)
	if err != nil {
		if hasCached {
			return cached.words, nil
		}
		return nil, err
	}
	if notModified {
		return cached.words, nil
	}
	if !found {
		// The replica served the blob but not the dictionary: recovery edge;
		// render placeholders rather than failing the tree.
		return nil, nil
	}
	s.dicts.mu.Lock()
	if len(s.dicts.hot) >= dictCacheCap {
		for k := range s.dicts.hot {
			delete(s.dicts.hot, k)
			break
		}
	}
	s.dicts.hot[tuple] = hotDictEntry{etag: dict.ETag, words: dict.Words}
	s.dicts.mu.Unlock()
	return dict.Words, nil
}
