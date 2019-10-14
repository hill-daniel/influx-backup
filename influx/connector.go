package influx

import (
	"fmt"
	"github.com/hill-daniel/influx-backup"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"os/exec"
	"strings"
)

// CreateSnapshot takes a snapshot from given influxdb and stores the files at the given path
func CreateSnapshot(data backup.Data) error {
	containerID, err := extractInfluxDbContainerID()
	if err != nil {
		return err
	}
	backupInfluxDb := fmt.Sprintf("docker exec %s influxd backup -portable -database %s %s", containerID, data.Database, data.MountedPath)
	out, err := exec.Command("/bin/sh", "-c", backupInfluxDb).Output()
	if err != nil {
		log.Infof("command output: %s", string(out))
		return errors.Wrapf(err, "failed to execute command: %s", backupInfluxDb)
	}
	return nil
}

func extractInfluxDbContainerID() (string, error) {
	grepContainerIDCmd := "docker ps | grep influxdb | cut -c 1-12"
	bytes, err := exec.Command("/bin/sh", "-c", grepContainerIDCmd).Output()
	if err != nil {
		return "", errors.Wrapf(err, "failed to execute command: %s", grepContainerIDCmd)
	}
	containerID := strings.TrimSpace(string(bytes))
	return containerID, nil
}
