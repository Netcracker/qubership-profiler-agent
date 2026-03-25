package pipe

import (
	"context"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/parser"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type IntegrationTestSuite struct {
	suite.Suite
	ctx           context.Context
	timestamp     time.Time
	fileVersionId string
}

const (
	Separator = "\n---\n"
	TestFile  = "ui5min.protocol"
)

func (suite *IntegrationTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(context.Background(), log.DEBUG)
	suite.preparePodFile(TestFile)
}

func (suite *IntegrationTestSuite) TearDownSuite() {
}

func TestIntegration(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}

func (suite *IntegrationTestSuite) pathToExpected(testFile string) string {
	pathToTestFile := filepath.Join("../", "../", "tests", "resources", "pipe", testFile)
	fileName, err := filepath.Abs(pathToTestFile)
	require.Nil(suite.T(), err)
	return fileName
}

func (suite *IntegrationTestSuite) pathToBin(podFile string) string {
	pathToPodFile := filepath.Join("../", "../", "tests", "resources", podFile)
	fileName, err := filepath.Abs(pathToPodFile)
	require.Nil(suite.T(), err)
	return fileName
}

func (suite *IntegrationTestSuite) testChunk(stream model.StreamType) *model.Chunk {
	pod := suite.preparePodFile(TestFile)
	c := suite.prepareChunk(pod, stream)
	require.NotNil(suite.T(), c)
	return c
}

func (suite *IntegrationTestSuite) preparePodFile(podFile string) *parser.ParsedPodDump {
	t := suite.T()
	fileName := suite.pathToBin(podFile)

	file, err := os.Open(fileName)
	require.Nil(t, err)

	tcpFile := parser.TcpFile{FileName: file.Name(), FilePath: fileName}
	data, err := parser.ParsePodTcpDump(suite.ctx, tcpFile)
	require.Nil(t, err)

	podData := &parser.ParsedPodDump{LoadedTcpData: data}
	require.NotNil(t, podData)
	//podData.ParseStreams(suite.ctx, false, "") // disable
	return podData
}

func (suite *IntegrationTestSuite) prepareChunk(p *parser.ParsedPodDump, streamType model.StreamType) *model.Chunk {
	for _, v := range p.Streams {
		if v.StreamType == streamType {
			return v
		}
	}
	return nil
}

func (suite *IntegrationTestSuite) readTestFile(testFile string, separator string) []string {
	fileName := suite.pathToExpected(testFile)
	s, err := readStringFile(fileName)
	require.Nil(suite.T(), err)
	list := strings.Split(stripLines(s), separator)
	if len(list) > 0 {
		if list[len(list)-1] == "" || strings.ToUpper(list[len(list)-1]) == "EOF" {
			list = list[:len(list)-1]
		}
	}

	return list
}

func OpenDataAsReader(data []byte, needBuffer bool) *PipeReader {
	//reader := bytes.NewReader(data)
	reader := NewByteSender(data)
	return NewPipeReader(reader, needBuffer)
}

func readStringFile(fileName string) (string, error) {
	path, err := filepath.Abs(fileName)
	if err != nil {
		return "", err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func stripLines(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n") // win
	s = strings.ReplaceAll(s, "\r", "\n")   // lin
	return strings.TrimSpace(s)
}

func (suite *IntegrationTestSuite) writeTestFile(filename string, writer func(*os.File)) {
	path := filepath.Join(".", filename)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	require.NoError(suite.T(), err)
	defer func() { _ = f.Close() }()

	writer(f)
}
