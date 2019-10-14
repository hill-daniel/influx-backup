package backup

// Backup is an abstraction for creating (dumping) a database snapshot.
type Backup interface {
	BackUp(databaseName string) (string, error)
}

// Data holds relevant backup information.
type Data struct {
	Database    string
	MountedPath string
	BackupPath  string
	BucketName  string
}

// Uploader is an abstraction for storing backup files.
type Uploader interface {
	Upload(content *FileContent) (storageLocation string, err error)
}

// FileContent is used in Uploader and holds information about the files to backup.
type FileContent struct {
	Key         string
	Content     *[]byte
	ContentType string
}
