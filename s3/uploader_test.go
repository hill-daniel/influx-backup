// +build integration

package s3_test

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/hill-daniel/influx-backup"
	"github.com/hill-daniel/influx-backup/s3"
	"log"
	"os"
	"strings"
	"testing"
)

const (
	bucketName     = "hillda-stuff"
	uploadFileName = "upload-integration-test.txt"
	envAwsLoadConf = "AWS_SDK_LOAD_CONFIG"
)

func Test_should_upload_file_to_s3(t *testing.T) {
	// session uses default credentials from ~/.aws/credentials and region from ~/.aws/config
	sharedSession := createSession()
	client := awss3.New(sharedSession)
	uploader := s3manager.NewUploader(sharedSession)
	keyProvider := s3.HexKeyProvider{}
	checkUploadFileDoesNotExist(client, keyProvider, t)
	binaryUploader := s3.NewBinaryUploader(uploader, keyProvider, bucketName)
	defer cleanup(client, keyProvider)
	fileContent := []byte("If you can read this, the upload was successful")
	bucketContent := &backup.FileContent{Key: uploadFileName, Content: &fileContent, ContentType: s3.BinaryContent}

	storageLocation, err := binaryUploader.Upload(bucketContent)

	if err != nil {
		t.Fatalf("failed to upload file, %v", err)
	}
	if !strings.HasSuffix(storageLocation, uploadFileName) {
		t.Fatalf("unexpected filename. Actual: %s expected: %s", storageLocation, uploadFileName)
	}
}

func createSession() *session.Session {
	sharedSession := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	return sharedSession
}

func checkUploadFileDoesNotExist(client *awss3.S3, keyProvider s3.HexKeyProvider, t *testing.T) {
	_, err := client.HeadObject(&awss3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    keyProvider.CreateKeyFor(uploadFileName),
	})
	if err == nil {
		t.Fatalf("cleanup of preceding test failed, object already exists")
	}
}

func cleanup(client *awss3.S3, keyProvider s3.HexKeyProvider) {
	input := &awss3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    keyProvider.CreateKeyFor(uploadFileName),
	}
	if _, err := client.DeleteObject(input); err != nil {
		log.Printf("failed to delete object %s from bucket %s", uploadFileName, bucketName)
	}
	cleanupEnv()
}

func cleanupEnv() {
	if err := os.Unsetenv(envAwsLoadConf); err != nil {
		log.Printf("failed to unset env variabke %s", envAwsLoadConf)
	}
}
