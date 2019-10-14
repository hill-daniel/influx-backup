# influx-backup
Take a snapshot of the given influxdb database, archive it (tar.gz) and upload archive to S3.

## Usage
- aws credentials and config are required (in ~/.aws directory)
- S3 bucket is required
- influxd running in a docker container with a backup directory mounted from the host system
- run with cmd/influx-backup/influx-backup -database=dbName -mountedPath=/var/lib/influxdb/backup -backupPath=/pathInHostSys/backup -bucketName=S3BucketName

## Whats happening?
- fetch docker container id with influx db runnning
- trigger influxd backup with docker execute, files will be stored in mountedPath 
- fetch backup files, gzip and upload to s3 from backupPath

## paths
- mountedPath -> directory in docker container
- backupPath -> the directory in the host system, mounted in the container