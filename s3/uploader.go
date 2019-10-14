package s3

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/hill-daniel/influx-backup"
	"github.com/pkg/errors"
)

const (
	// BinaryContent is default content type for binary files
	BinaryContent = "binary/octet-stream"
	// Gzip content type for files
	Gzip = "application/gzip"
)

// BinaryUploader uploads files to s3 bucket.
type BinaryUploader struct {
	uploader    *s3manager.Uploader
	keyProvider BucketKeyProvider
	bucketName  string
}

// NewBinaryUploader creates a new binary uploader.
func NewBinaryUploader(uploader *s3manager.Uploader, keyProvider BucketKeyProvider, bucketName string) BinaryUploader {
	return BinaryUploader{uploader: uploader, keyProvider: keyProvider, bucketName: bucketName}
}

// Upload uploads the given object as bytes to S3 for the given key
func (u BinaryUploader) Upload(content *backup.FileContent) (storageLocation string, err error) {
	reader := bytes.NewReader(*content.Content)
	key := u.keyProvider.CreateKeyFor(content.Key)
	result, err := u.uploader.Upload(&s3manager.UploadInput{
		Body:        reader,
		Bucket:      aws.String(u.bucketName),
		Key:         &key,
		ContentType: aws.String(content.ContentType)})
	if err != nil {
		err = errors.Wrapf(err, "failed to upload item with key %s to bucket %s", content.Key, u.bucketName)
		return storageLocation, err
	}
	return result.Location, nil
}
