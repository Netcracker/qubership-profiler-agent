package envconfig

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestByteSizeDecode(t *testing.T) {
	for raw, want := range map[string]ByteSize{
		"0":     0,
		"1234":  1234,
		"4MB":   4 << 20,
		"2GB":   2 << 30,
		"10GiB": 10 << 30,
		"64mb":  64 << 20,
		" 1 KB": 1 << 10,
	} {
		var b ByteSize
		require.NoError(t, b.Decode(raw), raw)
		assert.Equal(t, want, b, raw)
	}
	for _, raw := range []string{"", "MB", "-1KB", "1.5GB", "10EB"} {
		var b ByteSize
		assert.Error(t, b.Decode(raw), raw)
	}
}

func TestDurationThresholdsDecode(t *testing.T) {
	var d DurationThresholds
	require.NoError(t, d.Decode("100ms, 1s"))
	assert.Equal(t, DurationThresholds{100 * time.Millisecond, time.Second}, d)

	for _, raw := range []string{"", "1s", "1s,100ms", "0s,1s", "1s,1s,2s", "abc,1s"} {
		assert.Error(t, d.Decode(raw), raw)
	}
}

func TestCollectDefaults(t *testing.T) {
	t.Setenv("S3_ENDPOINT", "http://minio:9000")
	t.Setenv("S3_BUCKET", "profiler-data")
	t.Setenv("S3_ACCESS_KEY", "ak")
	t.Setenv("S3_SECRET_KEY", "sk")

	c, err := ParseCollect()
	require.NoError(t, err)
	assert.Equal(t, "/data", c.DataDir)
	assert.Equal(t, 1715, c.AgentPort)
	assert.Equal(t, 8081, c.InternalAPIPort)
	assert.Equal(t, 5*time.Minute, c.TimeBucket)
	assert.Equal(t, DurationThresholds{100 * time.Millisecond, time.Second}, c.DurationThresholds)
	assert.Equal(t, ByteSize(4<<20), c.SegmentRotationSize)
	// The loops must default ON: a collector that never seals, uploads, or
	// cleans up is not a collector (01 §6.1-§6.3).
	assert.Positive(t, c.SealCheckInterval)
	assert.Positive(t, c.UploadCheckInterval)
	assert.Positive(t, c.JanitorCheckInterval)
	assert.Equal(t, 15*time.Minute, c.HotRetention)
	assert.Equal(t, ByteSize(10<<30), c.ChunksStagingMaxBytes)
	assert.Equal(t, time.Hour, c.WalPurgeGrace)

	p, err := c.S3.Params()
	require.NoError(t, err)
	assert.Equal(t, "minio:9000", p.Endpoint)
	assert.False(t, p.UseSSL)
	assert.Equal(t, "profiler-data", p.BucketName)
}

func TestQueryDefaults(t *testing.T) {
	t.Setenv("S3_ENDPOINT", "https://s3.example.com")
	t.Setenv("S3_BUCKET", "profiler-data")
	t.Setenv("S3_ACCESS_KEY", "ak")
	t.Setenv("S3_SECRET_KEY", "sk")
	t.Setenv("COLLECTOR_HEADLESS_SVC", "profiler-collector-headless")

	q, err := ParseQuery()
	require.NoError(t, err)
	assert.Equal(t, 8080, q.ExternalAPIPort)
	assert.Equal(t, "profiler-collector-headless", q.CollectorService)
	assert.Equal(t, 8081, q.CollectorPort)
	assert.Equal(t, ByteSize(2<<30), q.MaxScanBytes)
	p, err := q.S3.Params()
	require.NoError(t, err)
	assert.True(t, p.UseSSL)
}

func TestTTLDecode(t *testing.T) {
	for raw, want := range map[string]TTL{
		"1d":   TTL(24 * time.Hour),
		"35d":  TTL(35 * 24 * time.Hour),
		"0d":   0,
		" 7 d": TTL(7 * 24 * time.Hour),
		"36h":  TTL(36 * time.Hour),
	} {
		var ttl TTL
		require.NoError(t, ttl.Decode(raw), raw)
		assert.Equal(t, want, ttl, raw)
	}
	for _, raw := range []string{"", "d", "-1d", "1.5d", "-24h", "abc"} {
		var ttl TTL
		assert.Error(t, ttl.Decode(raw), raw)
	}
}

func TestMaintainDefaults(t *testing.T) {
	t.Setenv("S3_ENDPOINT", "http://minio:9000")
	t.Setenv("S3_BUCKET", "profiler-data")
	t.Setenv("S3_ACCESS_KEY", "ak")
	t.Setenv("S3_SECRET_KEY", "sk")

	m, err := ParseMaintain()
	require.NoError(t, err)
	assert.Equal(t, 5*time.Minute, m.CheckInterval)
	assert.Equal(t, 5*time.Minute, m.TimeBucket)
	assert.Equal(t, 30*time.Minute, m.CompactionMinAge)
	assert.Equal(t, 4, m.CompactionMinFiles)
	// The delete grace must stay well above one discovery-plus-read round
	// (01 §6.6); 5m is the contract default (01 §9).
	assert.Equal(t, 5*time.Minute, m.CompactionDeleteGrace)
	assert.Equal(t, ByteSize(256<<20), m.CompactionMaxBytes)
	assert.Equal(t, TTL(24*time.Hour), m.RetentionShortCleanTTL)
	assert.Equal(t, TTL(7*24*time.Hour), m.RetentionNormalCleanTTL)
	assert.Equal(t, TTL(30*24*time.Hour), m.RetentionLongCleanTTL)
	assert.Equal(t, TTL(30*24*time.Hour), m.RetentionAnyErrorTTL)
	assert.Equal(t, TTL(7*24*time.Hour), m.RetentionCorruptedTTL)
	assert.Equal(t, TTL(35*24*time.Hour), m.RetentionDictionaryTTL)
}

func TestS3Required(t *testing.T) {
	// t.Setenv registers the restore; Unsetenv then truly clears the var —
	// envconfig's `required` accepts a set-but-empty value.
	for _, key := range []string{"S3_ENDPOINT", "S3_BUCKET", "S3_ACCESS_KEY", "S3_SECRET_KEY"} {
		t.Setenv(key, "")
		require.NoError(t, os.Unsetenv(key))
	}
	_, err := ParseCollect()
	assert.Error(t, err, "S3_ENDPOINT and S3_BUCKET stay required at parse time")

	// The credentials are validated at Params() time, not at parse time: the
	// parser cannot know which of the env/file sources the deployment uses.
	t.Setenv("S3_ENDPOINT", "http://minio:9000")
	t.Setenv("S3_BUCKET", "profiler-data")
	c, err := ParseCollect()
	require.NoError(t, err)
	_, err = c.S3.Params()
	assert.ErrorContains(t, err, "missing S3_ACCESS_KEY")
}

func TestS3FileCredentials(t *testing.T) {
	dir := t.TempDir()
	accessPath := filepath.Join(dir, "access-key")
	secretPath := filepath.Join(dir, "secret-key")
	// Secret files routinely end with a newline; it must not become part of
	// the credential.
	require.NoError(t, os.WriteFile(accessPath, []byte("file-ak\n"), 0o600))
	require.NoError(t, os.WriteFile(secretPath, []byte("file-sk"), 0o600))

	base := S3{Endpoint: "http://minio:9000", Bucket: "profiler-data"}

	s := base
	s.AccessKeyFile, s.SecretKeyFile = accessPath, secretPath
	p, err := s.Params()
	require.NoError(t, err)
	assert.Equal(t, "file-ak", p.AccessKeyID)
	assert.Equal(t, "file-sk", p.SecretAccessKey)

	s = base
	s.AccessKey, s.SecretKey = "env-ak", "env-sk"
	p, err = s.Params()
	require.NoError(t, err)
	assert.Equal(t, "env-ak", p.AccessKeyID)

	// Both sources for one credential is a misconfiguration, not a precedence
	// question: fail loudly instead of silently picking one.
	s = base
	s.AccessKey, s.AccessKeyFile, s.SecretKey = "env-ak", accessPath, "env-sk"
	_, err = s.Params()
	assert.ErrorContains(t, err, "both set")

	s = base
	s.AccessKeyFile = filepath.Join(dir, "does-not-exist")
	s.SecretKey = "env-sk"
	_, err = s.Params()
	assert.ErrorContains(t, err, "read S3_ACCESS_KEY_FILE")

	require.NoError(t, os.WriteFile(filepath.Join(dir, "empty"), []byte("\n"), 0o600))
	s = base
	s.AccessKeyFile = filepath.Join(dir, "empty")
	s.SecretKey = "env-sk"
	_, err = s.Params()
	assert.ErrorContains(t, err, "empty credential")
}
