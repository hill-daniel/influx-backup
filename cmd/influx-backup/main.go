package main

import (
	"flag"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/hill-daniel/influx-backup"
	"github.com/hill-daniel/influx-backup/gzip"
	"github.com/hill-daniel/influx-backup/influx"
	"github.com/hill-daniel/influx-backup/s3"
	log "github.com/sirupsen/logrus"
	"os"
)

const envLogLevel = "LOG_LEVEL"

func init() {
	lvl, err := log.ParseLevel(os.Getenv(envLogLevel))
	if err != nil {
		lvl = log.InfoLevel
	}
	log.SetFormatter(&log.TextFormatter{})
	log.SetLevel(lvl)
}

func main() {
	data := backup.Data{}
	flag.StringVar(&data.Database, "database", "myDbName", "database to backup")
	flag.StringVar(&data.MountedPath, "mountedPath", "/var/lib/influxdb/backup", "path for the backup dir, mounted in docker container")
	flag.StringVar(&data.BackupPath, "backupPath", "/Users/ec2user/influxdb/data/backup", "path for the backup dir on the host system")
	flag.StringVar(&data.BucketName, "bucketName", "myS3Bucket", "s3 bucket name for backup upload")
	flag.Parse()

	if err := influx.CreateSnapshot(data); err != nil {
		log.Fatalf("failed to create snapshot for docker influxdb, %v", err)
	}
	binaryUploader := createS3Uploader(data.BucketName)
	bb := createBackuper(binaryUploader)
	storageLocation, err := bb.BackUp(data.BackupPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("successfully dumped influxdb %s to s3 at %s", data.Database, storageLocation)
}

func createS3Uploader(bucketName string) *s3.BinaryUploader {
	sharedSession := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	uploader := s3manager.NewUploader(sharedSession)
	keyProvider := s3.HexKeyProvider{}
	binaryUploader := s3.NewBinaryUploader(uploader, keyProvider, bucketName)
	return &binaryUploader
}

func createBackuper(uploader backup.Uploader) backup.Backup {
	archiver := gzip.GzTarer{}
	bb := s3.NewBucketBackup(uploader, archiver)
	return bb
}
