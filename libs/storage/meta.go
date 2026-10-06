package model

import (
	"fmt"
	"time"
)

// -----------------------------------------------------------------------------

type PodInfo struct {
	PodId       string
	Namespace   string
	ServiceName string
	PodName     string
	ActiveSince time.Time
	LastRestart time.Time
	LastActive  time.Time
	Tags        map[string]string
}

func (pod *PodInfo) String() string {
	return fmt.Sprintf("%s/%s", pod.ServiceName, pod.PodName)
}

// -----------------------------------------------------------------------------

type PodRestart struct {
	PodId       string
	Namespace   string
	ServiceName string
	PodName     string
	RestartTime time.Time
	ActiveSince time.Time
	LastActive  time.Time
}

func (pod *PodRestart) String() string {
	return fmt.Sprintf("%s/%s/%d", pod.ServiceName, pod.PodName, pod.RestartTime.UnixMilli())
}

// -----------------------------------------------------------------------------

type Param struct {
	PodId       string
	PodName     string
	RestartTime time.Time
	ParamName   string
	ParamIndex  bool
	ParamList   bool
	ParamOrder  int
	Signature   string
}

// -----------------------------------------------------------------------------

type Dictionary struct {
	PodId       string
	PodName     string
	RestartTime time.Time
	Position    int
	Tag         string
}
