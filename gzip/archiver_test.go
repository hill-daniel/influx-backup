package gzip_test

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"fmt"
	backup "github.com/hill-daniel/influx-backup/gzip"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
)

func Test_should_gzip_and_tar_files_in_directory(t *testing.T) {
	path := "/tmp/test"
	extractPath := "/tmp/ex"
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to close io directory, %v", err)
		}
	}()
	defer func() {
		if err := os.RemoveAll(extractPath); err != nil {
			t.Errorf("failed to close io directory, %v", err)
		}
	}()
	archivePath := path + "/output.tar.gz"
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeTwoFiles(path); err != nil {
		t.Fatal(err)
	}
	gzTarer := backup.GzTarer{}

	if err := gzTarer.TarGz(archivePath, path); err != nil {
		t.Fatalf("failed to write archive from %s to %s, %v", path, archivePath, err)
	}

	// magic number at the beginning of a gz file: 0x1f8b.
	if err := checkGzFormat(archivePath); err != nil {
		t.Fatalf("written file is not in gz format")
	}
	fileNames, err := extractFileNames(archivePath, extractPath)
	if err != nil {
		t.Fatalf("error checking archive %v", err)
	}
	if len(fileNames) > 2 {
		t.Fatalf("archive contains more files than it should")
	}
	if !(fileNames["dat_0.txt"] && fileNames["dat_1.txt"]) {
		t.Fatalf("archive did not contain the desired files")
	}
}

func writeTwoFiles(path string) error {
	for i := 0; i < 2; i++ {
		fileContent := "hello\ngo" + strconv.Itoa(i) + "\n"
		bytes := []byte(fileContent)
		filePath := path + "/dat_" + strconv.Itoa(i) + ".txt"
		if err := ioutil.WriteFile(filePath, bytes, 0700); err != nil {
			fmt.Printf("failed to write file %s, %v", filePath, err)
		}
	}
	return nil
}

func readFirstTwoBytes(archivePath string) ([]byte, error) {
	file, err := os.Open(archivePath)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(file)
	firstTwoBytes, err := reader.Peek(2)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read bytes of %s", archivePath)
	}
	return firstTwoBytes, nil
}

func checkGzFormat(archivePath string) error {
	firstTwoBytes, err := readFirstTwoBytes(archivePath)
	if err != nil {
		return err
	}
	if !(firstTwoBytes[0] == 31 && firstTwoBytes[1] == 139) {
		return errors.New("data is not in gz format")
	}
	return nil
}

func extractFileNames(archivePath string, targetPath string) (map[string]bool, error) {
	if err := os.Mkdir(targetPath, 0700); err != nil {
		return nil, err
	}
	if err := Untar(targetPath, archivePath); err != nil {
		return nil, errors.Wrapf(err, "failed to untar files")
	}
	extractedDir, err := os.Open(targetPath)
	if err != nil {
		return nil, err
	}
	files, err := extractedDir.Readdir(0)
	if err != nil {
		return nil, err
	}
	fileNames := make(map[string]bool)
	for _, fileInfo := range files {
		fileNames[fileInfo.Name()] = true
	}
	return fileNames, nil
}

func Untar(dst string, src string) error {
	tarArchive, err := os.Open(src)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := tarArchive.Close(); err != nil {
			fmt.Printf("failed to close io tarArchive, %v", err)
		}
	}()

	gzipReader, err := gzip.NewReader(tarArchive)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := gzipReader.Close(); err != nil {
			fmt.Printf("failed to close io gzipReader, %v", err)
		}
	}()

	tarReader := tar.NewReader(gzipReader)

	for {
		archiveEntry, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		switch archiveEntry.Typeflag {
		case tar.TypeDir: // directory
			fmt.Println("creating: " + archiveEntry.Name)
			err = os.MkdirAll(archiveEntry.Name, 0700)
			if err != nil {
				return err
			}
		case tar.TypeReg: // regular file
			fmt.Println("extracting: " + archiveEntry.Name)
			file, err := os.Create(dst + "/" + archiveEntry.Name)
			if err != nil {
				return err
			}
			_, err = io.Copy(file, tarReader)
			if err != nil {
				return err
			}
			if err := file.Close(); err != nil {
				fmt.Printf("failed to close io directory, %v", err)
			}
		}
	}
	return nil
}
