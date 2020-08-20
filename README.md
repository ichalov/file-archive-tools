# file-archive-tools

A set of utilities that help maintain file archive (e.g. of raw photos or system backup tarballs).

* Download-Dispatcher.rakumod - a library to organize download queue over `wget` or similar
* file-tree-dedup.raku - compare two directories (similarly to `diff -r`), find the files that are identical in both dirs and delete them from second dir only
* pack-optical-disc.raku - find combinations of files in a directory that fit in optical disks of various sizes leaving minimal amount of free space on them
