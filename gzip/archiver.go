package gzip

import (
	"archive/tar"
	"compress/gzip"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
)

// Tarer is an abstraction for creating Tar archives.
type Tarer interface {
	TarGz(outFilePath string, inPath string) error
}

// GzTarer gzips and tars archives.
type GzTarer struct {
}

// TarGz and archives given files in path to a tar.gz file.
func (GzTarer) TarGz(outFilePath string, inPath string) error {
	file, err := os.Create(outFilePath)
	if err != nil {
		return errors.Wrapf(err, "failed to create file %s", outFilePath)
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Errorf("failed to close io file, %v", err)
		}
	}()

	gzipWriter := gzip.NewWriter(file)
	defer func() {
		if err = gzipWriter.Close(); err != nil {
			log.Errorf("failed to close io gzipWriter, %v", err)
		}

	}()

	tarWriter := tar.NewWriter(gzipWriter)
	defer func() {
		if err = tarWriter.Close(); err != nil {
			log.Errorf("failed to close io tarWriter, %v", err)
		}
	}()

	if err := iterateDir(inPath, tarWriter, func(currentPath string) bool {
		return currentPath == outFilePath
	}); err != nil {
		return err
	}
	log.Infof("tar.gz ok")
	return nil
}

func iterateDir(dirPath string, tw *tar.Writer, ignore func(currentPath string) bool) error {
	dir, err := os.Open(dirPath)
	if err != nil {
		return errors.Wrapf(err, "failed to open file %s", dirPath)
	}
	defer func() {
		if err = dir.Close(); err != nil {
			log.Errorf("failed to close io directory, %v", err)
		}
	}()

	files, err := dir.Readdir(0)
	if err != nil {
		return errors.Wrapf(err, "failed to read directory %s", dirPath)
	}

	for _, file := range files {
		currentPath := dirPath + "/" + file.Name()
		if file.IsDir() {
			if err = iterateDir(currentPath, tw, ignore); err != nil {
				return err
			}
		} else if !ignore(currentPath) {
			log.Infof("adding... %s\n", currentPath)
			if err := tarGzWrite(dirPath, currentPath, tw, file); err != nil {
				return err
			}
		}
	}
	return nil
}

func tarGzWrite(archiveDir string, path string, tarWriter *tar.Writer, fileInfo os.FileInfo) error {
	file, err := os.Open(path)
	if err != nil {
		return errors.Wrapf(err, "failed to open file %s", path)
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Errorf("failed to close io file, %v", err)
		}
	}()

	header := new(tar.Header)
	header.Name = path[len(archiveDir)+1:]
	header.Size = fileInfo.Size()
	header.Mode = int64(fileInfo.Mode())
	header.ModTime = fileInfo.ModTime()
	err = tarWriter.WriteHeader(header)
	if err != nil {
		return errors.Wrapf(err, "failed to write header")
	}
	_, err = io.Copy(tarWriter, file)
	if err != nil {
		return errors.Wrapf(err, "failed to copy header")
	}
	return nil
}
