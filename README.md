# Backup

This is my homemade backup solution.  It backs up your data to Amazon Glacier
and encrypts it along the way.

Project goals:

 - low-cost
 - ahead-of-time cost estimation
 - all data encrypted on remote store
 - concurrent backups are safe
 - progress meter
 - detection of moved files
 - handling for symbolic and hard links
 - deduplication

## "Quick" Start

You'll need Make, Gradle, and a Java JDK version 10 or higher.

Make sure you have a `~/.aws/credentials` file with your AWS credentials:

    [default]
    aws_access_key_id=ACCESS_KEY
    aws_secret_access_key=SECRET_KEY

Configure what files will be backed up:

    cp example-config.json ~/.backup-config.json

Then edit `~/.backup-config.json` to taste.

Build the project and show help:

    make
    ./dist/bin/bkup -h

To do a backup:

    ./dist/bin/bkup -b

## Known Issues

 - Restore is not implemented!  However, the tool can do "spot-checks" to test
   that a few random files were correctly backed-up.
 - The tool prompts for passwords and confirmation, so it is not suitable to
   use in automated scripts (e.g. cron).

## Notes on Cost and Resource Usage

This program uses three AWS services:

 - Glacier (for your file contents)
 - S3 (for the "index" that maps filesystem paths to contents)
 - DynamoDB (for reliable interaction with S3, since S3 only offers eventual
   consistency)

Most of the cost comes from uploading data to Glacier.  There is a per-request
charge that dominates costs when backing up lots of tiny files.

Running the tool incurs less-than-one-cent cost to S3 and Dynamo.  The tool
pre-computes an estimate of the Glacier costs and prompts for confirmation if
it will be more than a penny.  It also displays the monthly maintenance cost
for the backup.

Other notes:

 - The `--local` flag disables AWS entirely.  The backup and the index will be
   in /tmp.
 - The unit tests do not use AWS and can be run for free.

## Notes on Concurrency and Consistency

The backup process is safe to run on multiple computers simultaneously, or on
the same computer in parallel.  Parallel runs will "interfere" with each other
and cause slowdowns or process crashes, but they will never corrupt the backup.

Note that it is not possible to guarantee a consistent backup; filesystem APIs
simply don't offer enough promises.  There is always the chance that another
process modifies the filesystem in parallel with the backup process, resulting
in strange behavior.

Some sources of inconsistency:

 - Moving a file during the backup could cause the backup process to miss it
   entirely.
 - Writing to a file during the backup could cause a strange intermediate view
   of that file to be backed up.
 - Modifying file A followed by file B could result in the new version of B and
   the old version of A appearing in the backup.
 - The backup process uses timestamps to detect new versions of files.
   Modifying timestamps by hand might cause new versions of files to never be
   backed up.  (To help mitigate this, the backup process considers any change
   to the timestamp---even a decrease---to merit backing-up the file.)

Some things that _are_ ok, on POSIX filesystems:

 - If you create and replace files by moving a new version into place (which is
   how many programs ensure atomic updates), then only either the old version
   or the new version will be backed up.  There will not be an intermediate
   version with mixed changes.
 - If you delete a file during the backup, then the backup process will either
   backup the old version of the file or it will see the deletion.  It will not
   backup half the file.

## Notes on Security

This project uses [AESCrypt](https://www.aescrypt.com/java_aes_crypt.html) for
encryption.

In general, Amazon keeps your data relatively secure.  The purpose of the
encryption is to defend against data breaches and against general nosiness from
AWS engineers.

Your files are encrypted with individual passwords.  The individual passwords
are stored in the index in S3, so you must pick a strong password for the
index!  You can change your password later.  However, it is not clear to me
how long Amazon retains your data after a deletion, so you should not fudge
the password from the outset.

Because there is a one-to-one mapping between files and Glacier archives, some
of your usage patterns are exposed to a motivated attacker.
