# Backup

This is my homemade backup solution.

Project goals:

 - low-cost
 - cost estimation
 - all data encrypted on remote store
 - concurrent backups are safe
 - progress meter
 - detection of moved files
 - deduplication

## Quickstart

Configure:

    cp example-config.json ~/.backup-config.json

Then edit `~/.backup-config.json` to taste.

Build the project and do a backup:

    make
    ./dist/bin/bkup -b

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
