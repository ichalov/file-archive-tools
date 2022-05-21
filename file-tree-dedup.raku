#`(
  file-tree-dedup.raku
  Author: Victor Ichalov <ichalov@gmail.com>, 2020-08

  TODO:
    Add option to replace the duplicate files with symlinks to <dir0>
    Unpack tarballs in <dir0> for detailed comparison
    Implement recycled bin - move duplicate files there instead of just deleting
    Add a mode to remove files that also within a tarball in the same directory
    Add protection against looped symlink dirs if needed

)

my $script-description = Q:c:to/EOT/;

  This script removes files under <dir> that also exist under <dir0>.
  It handles <dir> recursively and deletes files only if their copies are placed
  in exactly same subdir of <dir0>. <dir0> may be given as "list:<filename>"
  where <filename> points to a text file containing output of a command like
        $ find . -type f -exec md5sum {'{}'} \;
  Options:
    --any-place - Relaxes the requirement of the same position and deletes file
      from <dir> if the file with same checksum is found anywhere under <dir0>
      (but has the same basename as in <dir0>).
    --any-basename - Further relax matching requirements and delete files based
      on checksum value only, even if they have different file name.
    --keep-file-symlinks - By default the script removes symlinks to files that
      are copies of correspondent <dir0> files (either the link leads to a file
      under <dir0> or not). The linked file is left intact in any case. This
      option prevents deletion of any file symlinks at all.
    --process-dir-symlinks - The script doesn't go into symlink dirs under <dir>
      unless this option is specified. Using this option may lead to deletion of
      files or the whole linked directory (even if it's outside <dir>) in case
      the files are identical to the contained under <dir0>. But it won't delete
      any files or dirs under <dir0> or linked from <dir0>.
    --shred - use shred command when deleting files (instead of just unlinking).
    --silent - Don't print logs of script actions on STDOUT.
  EOT

sub USAGE() {
  say $*USAGE;
  say $script-description;
}

# Global storage for <dir0>
my $d0;
my $d0-full;

# Global storages option presence
my Bool $_verbose;
my Bool $_any-basename;
my Bool $_keep-file-symlinks;
my Bool $_process-dir-symlinks;
my Bool $_shred;

# Global hashes for checksums and full paths after dereferencing symlinks of files
# under <dir0> .
# TODO: Better re-make using state variable, but difficult.
my %dir0-checksums = Empty;
my %dir0-full-paths = Empty;

# Global hash to keep the list of files and checksums from dir0 = list: argument
my %dir0-list = Empty;

sub MAIN(
  Str $dir0, Str $dir,
  Bool :$any-place,
  Bool :$any-basename,
  Bool :$keep-file-symlinks,
  Bool :$process-dir-symlinks,
  Bool :$shred,
  Bool :$silent,
) {
  if ( my $fno = $dir0 ~~ m/ ^ \s* 'list:' (.+) / ) {
    my $fn = $fno[0].Str;
    unless ( $fn.IO.f ) {
      die "Can't find source file {$fn}";
    }
    $d0 = './';
    %dir0-list = read-find-checksum-file( $fn );
  }
  else {
    $d0 = append-slash-to-dir( $dir0 );
    unless ( $d0.IO.d ) {
      die "Can't find directory {$d0}";
    }
    $d0-full = append-slash-to-dir( $d0.IO.resolve(:completely).Str );
  }

  my $d = append-slash-to-dir( $dir );
  unless ( $d.IO.d ) {
    die "Can't find directory {$d}";
  }

  $_verbose = ! $silent;
  $_keep-file-symlinks = $keep-file-symlinks;
  $_process-dir-symlinks = $process-dir-symlinks;
  $_shred = $shred;

  $_any-basename = $any-basename;
  if ( $any-basename && ! $any-place ) {
    die '--any-basename option should only be used along with --any-place';
  }

  process-sub-dir( $d, '',

    # Use different $proc functions depending on --any-place.
    $any-place ??
      sub ( $a, $b ) {
        CATCH {
          when is-io-exception( .Str ) {
            say "IO fault while comparing {$b}";
          }
        }
        # Calculate the whole list of <dir0> checksums and full paths on first
        # invocation.
        my Bool $files-deleted = False;
        if ( %dir0-list ) {
          for %dir0-list.kv -> $k, $v {
            push %dir0-checksums{ $v }, $k;
            %dir0-full-paths{ $k } = $v;
          }
        }
        if ( ! %dir0-checksums ) {
          process-sub-dir( $d0, '', sub ( $aa, $bb ) {
            CATCH {
              when is-io-exception( .Str ) {
                say "IO fault while accessing {$bb}";
              }
            }
            my $checksum = get-file-checksum( $bb );
            # Save file names as a list because there may be several files with
            # the same checksum under <dir0>
            push %dir0-checksums{ $checksum }, $bb;
            %dir0-full-paths{ $bb.IO.resolve(:completely).Str } = $bb;
            return False;
          } );
        }

        # NB: The shortcut with file size comparison is not possible here
        # because the <dir0> file to compare with is not definite yet.
        my $checksum = get-file-checksum( $b );
        if ( %dir0-checksums{ $checksum }:exists ) {
          # Prefer showing in logs the file that has the same basename
          my $src-file-name = %dir0-checksums{ $checksum }.sort(
            { .Str.IO.basename eq $b.IO.basename ?? 0 !! 1 }
          )[0];
          if ( %dir0-checksums{ $checksum }.elems > 1 ) {
            $src-file-name ~= ', ...';
          }
          if ( delete-file( $b, $checksum ) ) {
            say "{$b} -> remove ( { $src-file-name } )" if $_verbose;
            $files-deleted = True;
          }
        }
        return $files-deleted;
      }
    !!
      sub ( $a, $b ) {
        # Compare the files in <dir0> and <dir>. First compare sizes and then
        # proceed to checksum only if sizes are equal. When calculating
        # checksums, use different defaults for both calculations to prevent
        # removals if get-file-checksum() returns empty value.
        my Bool $files-deleted = False;
        if (
            %dir0-list && ( %dir0-list{ $a }:exists )
            ||
            ! %dir0-list && ( $a.IO.f && $a.IO.s == $b.IO.s )
        ) {
          CATCH {
            when is-io-exception( .Str ) {
              say "IO fault while comparing {$a}";
            }
          }
          my $cs = get-file-checksum( $b ) || '--';
          my $cs_0 = %dir0-list{ $a } || get-file-checksum( $a ) || '---';
          if ( $cs_0 eq $cs ) {
            if ( delete-file( $b ) ) {
              say "{$b} -> remove" if $_verbose;
              $files-deleted = True;
            }
          }
        }
        return $files-deleted;
      },

    :delete-dir-if-empty( &delete-dir-if-empty ),
    :dir-symlink-checks( &dir-symlink-checks ),
  );

  CATCH {
    # Only show usage on exceptions initiated directly in MAIN and not deeper
    # in callstack.
    if ( .gist ~~ m/ ^^ \s* in \s+ (.+) / ) {
      if ( $/[0].Str ~~ m/ ^ sub \s+ MAIN / ) {
        say "ERROR: " ~ .message ~ "\n\n";
        USAGE();
        say "\n\nERROR: " ~ .message ~ "\n";
        exit;
      }
    }
  }
}

# NB: This function is recursive and iterates through <dir> (or <dir0> in case
# of initial collection of checksums for --any-place option).
# $proc is how each leaf file should be processed (takes two full file names:
# first from <dir0> and second from <dir> ).
sub process-sub-dir(
  Str $root-dir, $sub-dir, Code $proc,
  Code :$delete-dir-if-empty?, Code :$dir-symlink-checks?,
) {
  my $dir = $root-dir ~ $sub-dir;

  my Bool $files-deleted;
  for $dir.IO.dir -> $fn0 {
    my $fn = $fn0.basename;
    my $fns = $sub-dir ?? $sub-dir ~ '/' ~ $fn !! $fn;
    my $fnf = $root-dir ~ $fns;
    my $fnf0 = $d0 ~ $fns; # NB: Use of a global !!!

    if ( $fnf.IO.d ) {
      if ( $dir-symlink-checks ) {
        if ( my $msg = $dir-symlink-checks( $fnf ) ) {
          say $msg;
          next;
        }
      }

      my $fd = process-sub-dir( $root-dir, $fns, $proc,
        :delete-dir-if-empty( $delete-dir-if-empty ),
        :dir-symlink-checks( $dir-symlink-checks ),
      );
      $files-deleted ||= $fd;
    }
    elsif ( $fnf.IO.f ) {
      my $fd = &$proc( $fnf0, $fnf );
      $files-deleted ||= ( $fd // False );
    }
  }

  # Delete current dir or dir symlink if it turns out to be empty after the
  # removals in $proc .
  if ( $delete-dir-if-empty && $files-deleted ) {
    if ( $delete-dir-if-empty( $dir ) ) {
      return True;
    }
  }
  return False;
}

# Perform additional checks before calling unlink() on any files.
sub delete-file ( Str $fnf, Str $checksum? ) {
  # Prevent deleting the files under <dir0> if they are in a dir symlinked from
  # target directory or --any-place option is used with two overlapping
  # directories.
  if ( is-dir-under-dir0( $fnf ) ) {
    say "{$fnf} -> skip (because it's under source dir {$d0})"
      if ( $_verbose );
    return False;
  }
  # Don't remove any symlinks if --keep-file-symlinks option is specified.
  if ( $_keep-file-symlinks && $fnf.IO.l ) {
    say "{$fnf} -> skip (file symlink)" if ( $_verbose );
    return False;
  }
  # Check that the deleted file is not linked from anywhere under <dir0>. This
  # is mainly to prevent deleting a directory outside of both <dir0> and <dir>
  # which is symlinked from both. But also can prevent deleting file from
  # <dir> that is symlinked from <dir0>.
  unless ( %dir0-full-paths || %dir0-list ) {
    process-sub-dir( $d0, '', sub ( $aa, $bb ) {
      %dir0-full-paths{ $bb.IO.resolve(:completely).Str } = $bb;
      return False;
    } );
  }
  if ( %dir0-full-paths{ $fnf.IO.resolve(:completely).Str }:exists ) {
    say "{$fnf} -> skip (linked from source dir "
      ~ %dir0-full-paths{ $fnf.IO.resolve(:completely).Str } ~ " )";
    return False;
  }
  # Compare base name of the file to delete with the list in %dir0-ckecksums (if
  # it's not empty which indicate --any-place option is present). Only delete
  # the file if a file with the same basename exists under <dir0> or
  # --any-basename option is specified.
  if ( %dir0-checksums && ! $_any-basename ) {
    my $basename = $fnf.IO.basename;
    my Bool $basename-match = False;
    for |%dir0-checksums{ $checksum } -> $file0 {
      if ( $basename eq $file0.IO.basename ) {
        $basename-match = True;
        last;
      }
    }
    unless ( $basename-match ) {
      say "{$fnf} -> skip (no matching basename among files with the same "
        ~ "checksum under <dir0>)";
      return False;
    }
  }

  if ( ! $_shred ) {
    unlink( $fnf );
  }
  else {
    run( '/usr/bin/shred', '-un2', $fnf );
  }
  return True;
}

sub delete-dir-if-empty( Str $_dir ) {
  my $dir = $_dir;
  my Bool $deletion-performed = False;
  # Only delete current dir if it turns out to be empty after the removals
  # in $proc calls in process-sub-dir() .
  # Only do that if $dir doesn't contain $d0 to prevent deleting empty
  # directories from <dir0> in --any-place $proc initialization run.
  if ( $dir !~~ m:i/ ^ $d0 / ) {
    if ( $dir.IO.l ) {
      # Remove symlinks to empty directories if they don't contain files and
      # --process-dir-symlinks is specified.
      if ( $_process-dir-symlinks && ! $dir.IO.dir.elems ) {
        my $linked-dir = $dir.IO.resolve(:completely).Str;
        unlink( $dir );
        say "{$dir} -> remove symlink to empty dir" if $_verbose;
        # Also delete the linked dir
        $dir = $linked-dir;
        $deletion-performed = True;
      }
    }
    my @deleted-dirs = rmdir( $dir );
    if ( $dir eq any( @deleted-dirs ) ) {
      say "{$dir} -> rmdir" if $_verbose;
      $deletion-performed = True;
    }
  }

  return $deletion-performed;
}

sub dir-symlink-checks( Str $fnf ) {
  # Skip going into symlinked dirs unless --process-dir-symlinks option
  if ( $fnf.IO.l && ! $_process-dir-symlinks ) {
    return "{$fnf} -> skip dir symlink"
  }
  # Skip dirs that are inside <dir0> when $root-dir isn't <dir0>
  if ( is-dir-under-dir0( $fnf ) ) {
    return "{$fnf} -> skip (because it's under source dir {$d0})"
  }
  return '';
}

sub is-dir-under-dir0 ( Str $dir, Str $dir0? ) {
  if ( %dir0-list ) {
    return False;
  }
  my $d0-full-local = $d0-full;
  if ( $dir0 ) {
    $d0-full-local = append-slash-to-dir( $dir0.IO.resolve(:completely).Str );
  }
  if ( $dir.IO.resolve(:completely).Str ~~ m:i/ $d0-full-local / ) {
    return True;
  }
  return False;
}

sub read-find-checksum-file ( Str $file ) returns Hash {
  my %res = Empty;

  for $file.IO.lines -> $l {
    if ( my $m = $l ~~ m/ ^ ( \w ** 32 ) \s+ (.+) $ / ) {
      %res{ $m[1].Str } = $m[ 0 ].Str;
    }
  }

  return %res;
}

sub append-slash-to-dir ( Str $dir ) returns Str {
  my $d = $dir;
  if ( $d !~~ m/ \/ \s* $ / ) {
    $d ~= '/';
  }
  return $d;
}

sub get-file-checksum( Str $file-name ) returns Str {
  return file_md5_hex( $file-name );
}

sub file_md5_hex ( Str $file-name ) returns Str {
  try require Digest::MD5;
  if ( ! $! ) {
    # TODO: Need to call .slurp( :bin ), but it causes big memory allocations.
    return ::('Digest::MD5').new.md5_hex: $file-name.IO.slurp;
  }

  # Only use the system utility if the module above is not available
  my $md5sum = '/usr/bin/md5sum';
  if ( ! $md5sum.IO.e ) {
    die "The script depends on Digest::MD5 module or {$md5sum} system utility "
      ~ "being installed.";
  }

  my $proc = run( $md5sum, $file-name, :out );
  my $ret = $proc.out.slurp;

  if ( my $m = $ret ~~ m:i/ ^ ( \w ** 32 ) \s+ $file-name / ) {
    return $m[0].Str;
  }
  else {
    die "Error calculating md5sum for {$file-name}";
  }
}

sub is-io-exception( Str $e ) {
  return True if ( $e ~~ m:i/ 'Error calculating md5sum for' / );
  # TODO: Include identification for Digest::MD5 errors
  return False;
}
