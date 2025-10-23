package model

type StatisticDownloadOption struct {
	TypeName DumpType `json:"typeName"`
	Uri      string   `json:"uri"`
}

type HeapDumpsStatistic struct {
	Date   int64  `json:"date"`
	Bytes  int64  `json:"bytes"`
	Handle string `json:"handle"`
}

type StatisticItem struct {
	Namespace         string                    `json:"namespace"`
	ServiceName       string                    `json:"serviceName"`
	PodName           string                    `json:"podName"`
	ActiveSinceMillis int64                     `json:"activeSinceMillis"`
	FirstSamleMillis  int64                     `json:"firstSampleMillis"`
	LastSampleMillis  int64                     `json:"lastSampleMillis"`
	DataAtStart       int64                     `json:"dataAtStart"`
	DataAtEnd         int64                     `json:"dataAtEnd"`
	CurrentBitrate    float64                   `json:"currentBitrate"`
	DownloadOptions   []StatisticDownloadOption `json:"downloadOptions"`
	OnlineNow         bool                      `json:"onlineNow"`
	HeapDumps         []HeapDumpsStatistic      `json:"heapDumps"`
}
