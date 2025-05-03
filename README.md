# Introduction
This program eases the use of Amazon S3 Glacier Archive for backing up your data by relieving some burdens like - maintaining records of what has been backed up, automatic retries on backup uploads in cases of intermittent internet connection failures, allowing resumption from last checkpoint (if the program was closed abruptly), uploading using multiple threads and ability to split the backup into several TAR files so that we need to only download few files reducing cost doing partial recovery.

More info at: https://TODO


# Requirements
1. Python - *v3.12.3*
2. All required Python modules are listed under `requirements.txt`.

# Setup
After you setup your Python environment according to requirements section above, you'll also need to setup AWS `config` and `credentials` files in `~/.aws` as according to [this](https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-files.html) link.

If you are testing, you can setup a local Minio as S3 server using `testing/docker-compose.yaml`.


# Usage
Use `python3 main.py --help` to list comands that are avaliable. Command output is also logged in files generated in `logs` folder.

## Backing up folders
To backup your folders, you may use the following command:

`python3 main.py backup --src-dirs /path/to/your/folder1 /path/to/your/folder2 --bucket=mybucket --encrypt --compression=gz /tmp/temp_folder/output.tar`

This command will recursively list the files in `/path/to/your/folder1` and `/path/to/your/folder2`, compress them into several tar files using `gzip` compression, encrypts the tar files with `ChaCha20` encryption algorithm using key `mykey` and uploads it to S3 bucket `mybucket` while assigning `Glacier Archive` class. The generated TAR files of this command will be saved in `/tmp/temp_folder/` and subsequently deleted after they are uploaded.

While testing with a local S3 server, you will want to pass `--test-run` option so that storage class `Deep Archive` is not specified as these may not be supported.

## Resuming an interrupted backup process
If your last backup was interrupted due to power or program faiilure, you can use the following command to resume it using the state database generated during the backup process:

`python3 main.py resume ./20250101_000000_backup_statedb.sqlite3`


## List processed files in state database
When you start a backup process, the program generates a state database in the same folder to record the processed files. To view the contents of a state database, you can use the `list` command as follows:

`python3 main.py list ./20250101_000000_backup_statedb.sqlite3`

You could also choose to see a collated view of only folders rather than individual files:

`python3 main.py list --collate=2 ./20250101_000000_backup_statedb.sqlite3`


## Decrypt downloaded TAR files
If you chose to encrypt your files during upload, naturally they must be decrypted before you can treat them as normal TAR files. Downloading your backup TAR files must be done manually using AWS Console as there's a fair bit of process in requesting and accessing the files with Deep Archive class assigned.

Once the encrypted files are downloaded in a folder, you can use the `decrypt` command as follows:

`python3 main.py decrypt ./20250101_000000_backup_statedb.sqlite3 /folder/of/encrypted/files`


## Synchronize state DB with remote S3 server
If you delete some files in the remote server and would like to re-sync your local state database with files in the remote server, you can use the `sync` command as follows:

`python3 main.py sync --bucket=mybucket ./20250101_000000_backup_statedb.sqlite3`


## Delete files from remote S3 server
If you want to delete some files in the remote server, you can use the `delete` command as follows:

`python main.py delete --bucket=mybucket --files 001_outputfile.tar.gz ./20250101_000000_backup_statedb.sqlite3`
