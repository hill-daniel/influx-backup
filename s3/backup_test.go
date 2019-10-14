package s3_test

import (
	"bufio"
	"bytes"
	"github.com/hill-daniel/influx-backup"
	"github.com/hill-daniel/influx-backup/gzip"
	"github.com/hill-daniel/influx-backup/s3"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
)

func Test_should_create_influx_dump_and_upload_gzipped_file_to_s3_cleaning_up_afterwards(t *testing.T) {
	testUploader := &testUploader{}
	archiver := &gzip.GzTarer{}
	backupPath := "/tmp/influx_snapshot"
	defer func() {
		if err := os.RemoveAll(backupPath); err != nil {
			t.Errorf("failed to close io directory, %v", err)
		}
	}()
	if err := createSomeFilesForBackup(backupPath); err != nil {
		t.Fatal(err)
	}
	bb := s3.NewBucketBackup(testUploader, archiver)

	storageLocation, err := bb.BackUp(backupPath)
	if err != nil {
		t.Fatal(err)
	}

	result := testUploader.result
	expected := "https://some.aws.url/snapshot/" + result.Key
	if storageLocation != expected {
		t.Fatalf("unexpected storage location. Actual: %s expected: %s", storageLocation, expected)
	}
	if len(*result.Content) == 0 {
		t.Fatal("content is empty")
	}
	if err := checkGzFormat(*result.Content); err != nil {
		t.Fatal("unexpected archive format in content")
	}
	if _, err := os.Open(backupPath); err == nil {
		t.Fatal("cleanup failed")
	}
}

func Test_should_not_cleanup_when_uploading_fails(t *testing.T) {
	testUploader := &testUploader{shouldFail: true}
	archiver := &gzip.GzTarer{}
	backupPath := "/tmp/influx_snapshot"
	defer func() {
		if err := os.RemoveAll(backupPath); err != nil {
			t.Errorf("failed to close io directory, %v", err)
		}
	}()
	if err := createSomeFilesForBackup(backupPath); err != nil {
		t.Fatal(err)
	}
	bb := s3.NewBucketBackup(testUploader, archiver)

	storageLocation, err := bb.BackUp(backupPath)

	if len(storageLocation) != 0 {
		t.Fatal("storageLocation should be empty")
	}
	if err == nil {
		t.Fatal("error should be propagated")
	}
	if err.Error() != "upload failed horribly" {
		t.Fatal("expected an other error text")
	}
	if _, err := os.Open(backupPath); err != nil {
		t.Fatal("cleanup should not have succeeded")
	}
}

func Test_should_not_cleanup_when_archiving_fails(t *testing.T) {
	testUploader := &testUploader{}
	failingArchiver := &failingArchiver{}
	backupPath := "/tmp/influx_snapshot"
	defer func() {
		if err := os.RemoveAll(backupPath); err != nil {
			t.Errorf("failed to close io directory, %v", err)
		}
	}()
	if err := createSomeFilesForBackup(backupPath); err != nil {
		t.Fatal(err)
	}
	bb := s3.NewBucketBackup(testUploader, failingArchiver)

	storageLocation, err := bb.BackUp(backupPath)

	if len(storageLocation) != 0 {
		t.Fatal("storageLocation should be empty")
	}
	if err == nil {
		t.Fatal("error should be propagated")
	}
	if !strings.HasPrefix(err.Error(), "failed to archive files, however backup was created") {
		t.Fatalf("expected an other error text, not %s", err.Error())
	}
	if _, err := os.Open(backupPath); err != nil {
		t.Fatal("cleanup should not have succeeded")
	}
}

func createSomeFilesForBackup(backupPath string) error {
	if err := os.MkdirAll(backupPath, 0700); err != nil {
		return errors.Wrapf(err, "failed to create directory %s", backupPath)
	}
	for i := 0; i < 2; i++ {
		fileContent := "hello\ngo" + strconv.Itoa(i) + "\n"
		contentBytes := []byte(fileContent)
		filePath := backupPath + "/dat_" + strconv.Itoa(i) + ".txt"
		err := ioutil.WriteFile(filePath, contentBytes, 0700)
		if err != nil {
			return errors.Wrapf(err, "failed to write file %s", filePath)
		}
	}
	return nil
}

type testUploader struct {
	result     *backup.FileContent
	shouldFail bool
}

func (u *testUploader) Upload(content *backup.FileContent) (storageLocation string, err error) {
	if u.shouldFail {
		return "", errors.New("upload failed horribly")
	}
	u.result = content
	return "https://some.aws.url/snapshot/" + content.Key, nil
}

// magic number at the beginning of a gz file: 0x1f8b.
func checkGzFormat(contentBytes []byte) error {
	bytesReader := bytes.NewReader(contentBytes)
	buffer := bufio.NewReader(bytesReader)
	peek, _ := buffer.Peek(2)
	if !(peek[0] == 31 && peek[1] == 139) {
		return errors.New("data is not in gz format")
	}
	return nil
}

type failingArchiver struct {
}

func (failingArchiver) TarGz(outFilePath string, inPath string) error {
	return errors.New("failed to archive")
}
