package data

import (
	"fmt"
	"math/rand"
	"time"
)

// ----------------------------------------------------------------------------------

const (
	charset = "abcdefghijklmnopqrstuvwxyz0123456789"
)

var (
	random = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func (cfg Config) Namespace(i int) string {
	return fmt.Sprintf("%s-%d", cfg.Prefixes.NS, i)
}

func (cfg Config) ServiceName(i int) string {
	return fmt.Sprintf("%s-%d", cfg.Prefixes.Service, i)
}

func (cfg Config) PodName(serviceName string) string {
	replica := randomString(10)
	pod := randomString(5)
	return fmt.Sprintf("%s-%s-%s", serviceName, replica, pod)
}

func randomString(size int) []byte {
	replicaRandom := make([]byte, size)
	for i := range replicaRandom {
		replicaRandom[i] = charset[random.Intn(len(charset))]
	}
	return replicaRandom
}
