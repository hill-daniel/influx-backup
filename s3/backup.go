package s3

import (
	"bufio"
	"github.com/hill-daniel/influx-backup"
	"github.com/hill-daniel/influx-backup/gzip"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

const unixTimestampFormat = "20060102150405"

// BucketBackup will gzip the snapshot files and upload them to S3.
// The created archive and snapshot files are removed after success.
type BucketBackup struct {
	uploader backup.Uploader
	archiver gzip.Tarer
}

// NewBucketBackup creates a new BucketBackup
func NewBucketBackup(uploader backup.Uploader, archiver gzip.Tarer) *BucketBackup {
	return &BucketBackup{uploader: uploader, archiver: archiver}
}

// BackUp tars, gzips given dir and uploads it to an s3 bucket.
func (d BucketBackup) BackUp(backupDirPath string) (string, error) {
	archivePath, err := d.archive(backupDirPath)
	if err != nil {
		return "", err
	}
	key := archivePath[len(backupDirPath)+1:]
	storageLocation, err := d.uploadToS3(key, archivePath)
	if err != nil {
		return "", err
	}
	if err := cleanup(backupDirPath); err != nil {
		log.Error(err)
	}
	return storageLocation, nil
}

func (d BucketBackup) archive(inPath string) (string, error) {
	t := time.Now()
	timestamp := t.Format(unixTimestampFormat)
	archivePath := inPath + "/dump_" + timestamp + ".tar.gz"
	if err := d.archiver.TarGz(archivePath, strings.TrimRight(inPath, "/")); err != nil {
		return "", errors.Wrapf(err, "failed to archive files, however backup was created")
	}
	return archivePath, nil
}

func (d BucketBackup) uploadToS3(key string, archivePath string) (string, error) {
	archiveFile, err := os.Open(archivePath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to open file %s", archivePath)
	}
	archiveReader := bufio.NewReader(archiveFile)
	content, err := ioutil.ReadAll(archiveReader)
	if err != nil {
		return "", errors.Wrapf(err, "failed to open read archive %s", archivePath)
	}
	bucketContent := &backup.FileContent{Key: key, ContentType: Gzip, Content: &content}
	return d.uploader.Upload(bucketContent)
}

func cleanup(path string) error {
	if path == "/" {
		return errors.New("root path provided, not going to cleanup")
	}
	if err := os.RemoveAll(path); err != nil {
		return errors.Wrapf(err, "failed to cleanup files, however backup was created and uploaded")
	}
	return nil
}
