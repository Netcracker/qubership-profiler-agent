package integration

import (
	"context"
	"fmt"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator"
	"github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/server"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestEmulator(t *testing.T) {
	//ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.DEBUG))
	ctx := log.SetLevel(context.Background(), log.DEBUG)

	t.Run("start", func(t *testing.T) {
		prepareServer(t, ctx)

		ac, err := prepareAgent(t, ctx)

		err = ac.InitializeConnection(100605, "test_namespace", "test_service", "test_pod")
		assert.Nil(t, err)
		fmt.Println(time.Now())
		time.Sleep(1 * time.Second) // wait for mock server
		//assert.Equal(t, "test+", serverListener.GetNamespace())

	})

	//netstat -anp TCP | grep 8001
}

func prepareServer(t *testing.T, ctx context.Context) {
	serverOpts := server.ConnectionOpts{
		ProtocolPort: 1715,
		Timeout: io.TcpTimeout{
			ConnectTimeout: 10 * time.Second,
			SessionTimeout: 60 * time.Second,
			ReadTimeout:    40 * time.Second,
			WriteTimeout:   2 * time.Second,
		},
	}

	serverListener := CreateMockServerListener()
	sc := server.PrepareServer(ctx, serverOpts, serverListener)
	assert.NotNil(t, sc)
	go func() {
		err := sc.Start(ctx)
		assert.Nil(t, err)
	}()
	time.Sleep(100 * time.Millisecond) // wait for mock server
}

func prepareAgent(t *testing.T, ctx context.Context) (*emulator.AgentConnection, error) {
	clientOpts := emulator.ConnectionOpts{
		ProtocolAddress: "localhost:1715",
		Timeout: io.TcpTimeout{
			ConnectTimeout: 10 * time.Second,
			SessionTimeout: 20 * time.Second,
			ReadTimeout:    2 * time.Second,
			WriteTimeout:   2 * time.Second,
		},
	}
	agentListener := CreateMockAgentListener()
	ac := emulator.PrepareAgent(ctx, nil, agentListener, "testPod")
	assert.NotNil(t, ac)

	fmt.Println(time.Now())
	err := ac.Prepare(clientOpts).Connect()
	assert.Nil(t, err)
	return ac, err
}
