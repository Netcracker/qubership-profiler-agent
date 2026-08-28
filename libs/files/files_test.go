package files

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	ctx = context.Background()
)

func TestDir(t *testing.T) {
	basePath, err := os.Getwd()
	assert.Nil(t, err)

	path1, err := filepath.Abs(filepath.Join(basePath, "test"))
	assert.Nil(t, err)
	err = CheckDir(ctx, path1)
	assert.Nil(t, err)

	path2 := filepath.Join(basePath, "test", "test2", "test3")
	err = CheckDir(ctx, path2)
	assert.Nil(t, err)

	path3 := filepath.Join(basePath, "test", "testB")
	err = CheckDir(ctx, path3)
	assert.Nil(t, err)

	size, err := FileSize(ctx, path2)
	assert.Nil(t, err)
	assert.True(t, size >= 0) // 0 for Win, 4096 for lin

	_, err = FileSize(ctx, path2+"bad")
	assert.ErrorContains(t, err, "test3bad")

	files, err := List(ctx, path1)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(files))
	assert.True(t, files[0].IsDir())
	assert.Equal(t, "test2", files[0].Name())
	assert.True(t, files[1].IsDir())
	assert.Equal(t, "testB", files[1].Name())

	err = ClearDirectory(ctx, path1)
	assert.Nil(t, err)

	files, err = List(ctx, path1)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(files))

	err = ClearDirectory(ctx, path1) // List will recreate directory
	assert.Nil(t, err)
}

func TestCheckFile(t *testing.T) {
	t.Run("unexist", func(t *testing.T) {
		unexistFileName := "./unexist.txt"
		err := CheckFile(unexistFileName)
		assert.ErrorContains(t, err, "can't get info")
	})

	t.Run("is dir", func(t *testing.T) {
		someDir, err := os.Getwd()
		assert.Nil(t, err)
		err = CheckFile(someDir)
		assert.ErrorContains(t, err, "is not a file")
	})

	t.Run("valid", func(t *testing.T) {
		execFile, err := os.Executable()
		assert.Nil(t, err)
		err = CheckFile(execFile)
		assert.Nil(t, err)
	})
}

type TestObj struct {
	Foo string `yaml:"foo"`
	Bar int    `yaml:"bar"`
}

func TestParseYamlFile(t *testing.T) {
	testDir := "../tests/resources"

	t.Run("unexist", func(t *testing.T) {
		unexistFileName := "./unexist.txt"
		testObj := TestObj{}
		err := ParseYamlFile(unexistFileName, &testObj)
		assert.ErrorContains(t, err, "can't get info")
	})

	t.Run("is dir", func(t *testing.T) {
		testObj := TestObj{}
		err := ParseYamlFile(testDir, &testObj)
		assert.ErrorContains(t, err, "is not a file")
	})

	t.Run("not yaml", func(t *testing.T) {
		testObj := TestObj{}
		err := ParseYamlFile(path.Join(testDir, "not_yaml_config.txt"), &testObj)
		assert.ErrorContains(t, err, "cannot unmarshal")
	})

	t.Run("unexist fields", func(t *testing.T) {
		testObj := TestObj{}
		err := ParseYamlFile(path.Join(testDir, "unexist_fields.yaml"), &testObj)
		assert.ErrorContains(t, err, "field unexist_field not found in type files.TestObj")
	})

	t.Run("valid", func(t *testing.T) {
		testObj := TestObj{}
		expectedObj := TestObj{
			Foo: "empty",
			Bar: 1,
		}
		err := ParseYamlFile(path.Join(testDir, "valid_config.yaml"), &testObj)
		assert.NoError(t, err)
		assert.Equal(t, expectedObj, testObj)
	})
}
