# Introduction
This program eases the use of Amazon S3 Glacier Archive for backing up your data by relieving some burdens like - maintaining records of what has been backed up, automatic retries on backup uploads in cases of intermittent internet connection failures, allowing resumption from last checkpoint (when the program was closed abruptly), uploading using multiple threads and ability to split the backup into several TAR files so that only few files need to be downloaded - reducing cost when doing partial recovery.

More info at: https://TODO


# Requirements
1. Python - *v3.12.3*
2. All required Python modules are listed under `requirements.txt`.

# Setup
After settings up your Python environment according to requirements section above, you'll also need to setup AWS `config` and `credentials` files in `~/.aws` as according to [this](https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-files.html) link.

If you are testing instead, you can setup a local Minio as S3 server using `testing/docker-compose.yaml`. Then `~/.aws` will need to be edited accordingly.

# Important things to know
* This program only supports full backup (and not incremental backup).
* State of your backup is stored in a generated `.sqlite3` file. Keep this file secured.
* Encyption is supported using `ChaCha20` algorithm that is enabled by default. The encryption key is stored in the generated state database. Nonce/Initialization Vector is the filename of the encrypted TAR, so don't rename your TAR files until they have been decrypted.
* Unless your files are all/mostly documents, you might want to keep compression disabled (default) as it might take a lot of compute and memory resources.
* Uploads are multi-threaded and if all fail due to network problems, the program will retry infinite number of times.
* Keep `--num-upload-workers` small (no more than 2) unless you have upload bandwidth of about 100 Mbits/secs.

# Usage
Use `python3 main.py --help` to list comands that are avaliable. Command output is also logged in `main.log` generated under `logs` folder.

## Backing up folders
To backup your folders, you may use the following command:

`python3 main.py backup --src-dirs /path/to/your/folder1 /path/to/your/folder2 --bucket=mybucket --encrypt --compression=gz /tmp/temp_folder/output.tar`

This command will recursively list the files in `/path/to/your/folder1` and `/path/to/your/folder2`, compress them into several tar files using `gzip` compression, encrypts the tar files with `ChaCha20` encryption algorithm using key `mykey` and uploads it to S3 bucket `mybucket` while assigning `Glacier Archive` class. The generated TAR files of this command will be saved in `/tmp/temp_folder/` and subsequently deleted after they are uploaded.

While testing with a local Minio S3 server, you will want to pass `--test-run` option so that unsupported storage class `Deep Archive` is not specified.

## Resuming an interrupted backup process
If your last backup was interrupted due to power or program faiilure, you can use the command similar to the following to resume it using the state database generated during the backup process:

`python3 main.py resume ./20250101_000000_backup_statedb.sqlite3`


## List processed files in state database
When you start a backup process, the program generates a state database in the same folder to record the processed files. To view the contents of a state database, you can use the `show` command as follows:

`python3 main.py show ./20250101_000000_backup_statedb.sqlite3`

You could also choose to see a collated view of only folders rather than individual files:

`python3 main.py show --collate=2 ./20250101_000000_backup_statedb.sqlite3`


## Decrypt downloaded TAR files
If you chose to encrypt your files during upload, naturally they must be decrypted before you can treat them as normal TAR files. Downloading your backup TAR files must be done manually using AWS Console as there's a fair bit of process involved.

Once the encrypted files are downloaded in a local folder, you can use the `decrypt` command as follows:

`python3 main.py decrypt ./20250101_000000_backup_statedb.sqlite3 /folder/of/encrypted/files`


## Synchronize state DB with remote S3 server
If you delete some files in the remote server and would like to re-sync your local state database with files in the remote server, you can use the `sync` command as follows:

`python3 main.py sync --bucket=mybucket ./20250101_000000_backup_statedb.sqlite3`


## Delete files from remote S3 server
If you want to delete some files in the remote server, you can use the `delete` command as follows:

`python main.py delete --bucket=mybucket --files 001_outputfile.tar.gz ./20250101_000000_backup_statedb.sqlite3`
