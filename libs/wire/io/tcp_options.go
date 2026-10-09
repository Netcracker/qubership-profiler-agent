package io

import "time"

type (
	TcpTimeout struct {
		ConnectTimeout time.Duration
		SessionTimeout time.Duration
		ReadTimeout    time.Duration
		WriteTimeout   time.Duration
	}
)
