# file-archive-tools

A set of utilities that help maintain file archive (e.g. of raw photos or system backup tarballs).

* file-tree-dedup.raku - compare two directories (similarly to `diff -r`), find the files that are identical in both dirs and delete them from second dir only
* Download-Dispatcher.rakumod - a library to organize download queue over `wget` or similar