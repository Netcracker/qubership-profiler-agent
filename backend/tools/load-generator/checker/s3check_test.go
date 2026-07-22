package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fastTimers is an accelerated-soak-shaped timer set, small enough that test
// buckets settle within minutes of fake time.
func fastTimers() s3Timers {
	return s3Timers{
		timeBucket:            time.Minute,
		timeBucketGrace:       10 * time.Second,
		sealCheckInterval:     5 * time.Second,
		uploadCheckInterval:   10 * time.Second,
		maintainCheckInterval: time.Minute,
		compactionMinAge:      3 * time.Minute,
		compactionDeleteGrace: time.Minute,
		settleSlack:           time.Minute,
		compactionMinFiles:    4,
		smallFileBytes:        1 << 20,
	}
}

// sealKey renders a seal-produced object key for one bucket.
func sealKey(class string, bucketStart time.Time, replica string, seq int) string {
	stamp := bucketStart.UTC().Format(s3KeyStamp)
	return fmt.Sprintf("parquet/v1/%s/%s/%s-abc123-%s-%s-%s-%d.parquet",
		class, bucketStart.UTC().Format("2006/01/02/15"), replica, stamp, stamp, stamp, seq)
}

func TestParseParquetObjectKey(t *testing.T) {
	bucket := time.Date(2026, 7, 16, 10, 5, 0, 0, time.UTC)
	key, ok := parseParquetObjectKey(s3Object{Key: sealKey("normal_clean", bucket, "collector-0", 1), Size: 512})
	require.True(t, ok)
	assert.Equal(t, "normal_clean", key.class)
	assert.Equal(t, "2026/07/16/10", key.hour)
	assert.Equal(t, bucket.UnixMilli(), key.bucketStartMs)
	assert.Equal(t, "collector-0", key.replica, "a replica with a dash parses from the right")

	_, ok = parseParquetObjectKey(s3Object{Key: "parquet/v1/normal_clean/2026/07/16/10/garbage"})
	assert.False(t, ok, "a foreign object is skipped, not an error")
	_, ok = parseParquetObjectKey(s3Object{Key: "wal/v1/whatever"})
	assert.False(t, ok)
}

func TestCompactionDeadlines(t *testing.T) {
	timers := fastTimers()
	bucketEnd := time.Date(2026, 7, 16, 10, 6, 0, 0, time.UTC)

	visible := timers.objectsVisibleAt(bucketEnd)
	assert.Equal(t, bucketEnd.Add(10*time.Second+5*time.Second+10*time.Second+time.Minute), visible)

	// compactionMinAge (3m) dominates the visibility chain (~1m25s), then two
	// maintain passes (compact, then delete the inputs after the grace), the
	// delete grace, and the slack stack on top.
	due := timers.compactionDueAt(bucketEnd)
	assert.Equal(t, bucketEnd.Add(3*time.Minute).Add(2*time.Minute+time.Minute+time.Minute), due)
}

// s3StateWith builds an s3state holding one listing taken at `at`.
func s3StateWith(at time.Time, objects []s3Object) *s3state {
	st := newS3State(fastTimers(), time.Hour)
	st.append(newS3Sample(at, objects, st.timers))
	return st
}

func TestCompactionKeepsUp(t *testing.T) {
	timers := fastTimers()
	now := time.Now()
	// A bucket comfortably past its compaction deadline.
	oldBucket := now.Add(-timers.compactionDueAt(time.Time{}).Sub(time.Time{})).Add(-2 * timers.timeBucket)
	oldBucket = oldBucket.Truncate(time.Minute)
	// A bucket that just ended: not judged.
	freshBucket := now.Truncate(time.Minute).Add(-timers.timeBucket)

	var objects []s3Object
	for seq := 0; seq < 5; seq++ { // 5 sealed files ≥ trigger of 4
		objects = append(objects, s3Object{Key: sealKey("normal_clean", oldBucket, "collector-0", seq), Size: 4 << 20})
	}
	for seq := 0; seq < 6; seq++ {
		objects = append(objects, s3Object{Key: sealKey("normal_clean", freshBucket, "collector-0", seq), Size: 4 << 20})
	}

	fs := s3StateWith(now, objects).checkCompaction(now, nil)
	require.Len(t, fs, 1, "only the settled bucket is judged")
	assert.Contains(t, fs[0].msg, "5 sealed objects")

	// Residue below the trigger passes; compacted outputs never count.
	objects = objects[:0]
	for seq := 0; seq < 3; seq++ {
		objects = append(objects, s3Object{Key: sealKey("normal_clean", oldBucket, "collector-0", seq), Size: 4 << 20})
	}
	objects = append(objects, s3Object{Key: sealKey("normal_clean", oldBucket, maintainReplica, 0), Size: 64 << 20})
	assert.Empty(t, s3StateWith(now, objects).checkCompaction(now, nil))
}

func TestCompactionNeedsAPostDeadlineListing(t *testing.T) {
	// The phase-5 race: a bucket crosses its deadline at due, the checker
	// evaluates at due+ε, but the newest listing is from due−75s — maintain
	// may have compacted the group in between. Stale evidence must not
	// judge; a listing taken after the deadline may.
	timers := fastTimers()
	now := time.Now()
	oldBucket := now.Add(-timers.compactionDueAt(time.Time{}).Sub(time.Time{})).Add(-timers.timeBucket)
	oldBucket = oldBucket.Truncate(time.Minute)
	due := timers.compactionDueAt(oldBucket.Add(timers.timeBucket))

	var objects []s3Object
	for seq := 0; seq < 5; seq++ {
		objects = append(objects, s3Object{Key: sealKey("normal_clean", oldBucket, "collector-0", seq), Size: 4 << 20})
	}

	stale := s3StateWith(due.Add(-75*time.Second), objects)
	assert.Empty(t, stale.checkCompaction(due.Add(time.Second), nil),
		"a pre-deadline listing is stale evidence and must not judge")

	fresh := s3StateWith(due.Add(30*time.Second), objects)
	assert.NotEmpty(t, fresh.checkCompaction(due.Add(time.Minute), nil),
		"a post-deadline listing showing the backlog still latches")
}

func TestSmallFileShareSlidingWindow(t *testing.T) {
	timers := fastTimers()
	now := time.Now()
	// One hour whose last bucket is far past its compaction deadline.
	hourStart := now.Add(-3 * time.Hour).Truncate(time.Hour)
	lastBucket := hourStart.Add(59 * time.Minute)

	mkObjects := func(small, large int) []s3Object {
		var out []s3Object
		for i := 0; i < small; i++ {
			out = append(out, s3Object{Key: sealKey("short_clean", lastBucket, "collector-0", i), Size: 1 << 10})
		}
		for i := 0; i < large; i++ {
			out = append(out, s3Object{Key: sealKey("short_clean", lastBucket, "collector-0", 100+i), Size: 4 << 20})
		}
		return out
	}

	st := newS3State(timers, time.Hour)
	// An early drop (compaction worked once)…
	st.append(newS3Sample(now.Add(-40*time.Minute), mkObjects(8, 2), timers))
	st.append(newS3Sample(now.Add(-30*time.Minute), mkObjects(2, 8), timers))
	// …must not mask monotonic growth afterwards.
	st.append(newS3Sample(now.Add(-20*time.Minute), mkObjects(4, 8), timers))
	st.append(newS3Sample(now.Add(-10*time.Minute), mkObjects(6, 8), timers))

	assert.Empty(t, st.checkSmallFileShare(now, nil),
		"the early drop keeps the whole-window series non-monotonic")

	// The window slides: once the early samples age out, the growth is bare.
	st.window = 25 * time.Minute
	st.append(newS3Sample(now, mkObjects(8, 8), timers))
	fs := st.checkSmallFileShare(now, nil)
	require.Len(t, fs, 1, "monotonic small-file growth inside the sliding window latches")
	assert.Contains(t, fs[0].subject, "short_clean/")
}

func TestSmallFileShareWaitsForTheHour(t *testing.T) {
	timers := fastTimers()
	now := time.Now()
	// The hour's newest bucket ended seconds ago: the hour is not judged.
	lastBucket := now.Truncate(time.Minute).Add(-timers.timeBucket)

	st := newS3State(timers, time.Hour)
	for i := 0; i < 4; i++ {
		at := now.Add(time.Duration(i-4) * time.Minute)
		st.append(newS3Sample(at, []s3Object{
			{Key: sealKey("short_clean", lastBucket, "collector-0", i), Size: 1 << 10},
		}, timers))
	}
	assert.Empty(t, st.checkSmallFileShare(now, nil))
}
