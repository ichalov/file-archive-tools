#`(
  file-tree-dedup.raku
  Author: <ichalov@gmail.com>, 2020-08-12

  TODO:
    Add option to replace the duplicate files with symlinks to <dir0>
    Unpack tarballs in <dir0> for detailed comparison
    Add a mode to remove files that also within a tarball in the same directory
    Split into --remove-file-symlinks and --remove-dir-symlinks
    Add protection against looped symlink dirs if needed
    Use Digest::MD5 to improve speed on small files

)

my $script-description = Q:c:to/EOT/;

  This script removes files under <dir> that also exist under <dir0>.
  It handles <dir> recursively and deletes files only if their copies are placed
  in exactly same subdir of <dir0>.
  Options:
    --any-place - Relaxes the requirement of the same position and deletes file
      from <dir> if the file with same md5sum is found anywhere under <dir0>.
    --remove-symlinks - The script doesn't remove symlinks unless this option
      specified. It won't remove symlinks referencing files under <dir0> even if
      the option is present.
    --silent - Don't print logs of all actions on STDOUT.
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
my Bool $_remove-symlinks;

# Global hash for md5sums of files in <dir0> in case of --any-place.
# TODO: Better re-make using state variable, but difficult.
my %dir0-md5sums = Empty;


sub MAIN(
  Str $dir0, Str $dir,
  Bool :$any-place, Bool :$silent, Bool :$remove-symlinks
) {
  $d0 = append-slash-to-dir( $dir0 );
  unless ( $d0.IO.d ) {
    die "Can't find directory {$d0}";
  }
  $d0-full = append-slash-to-dir( $d0.IO.resolve(:completely).Str );

  my $d = append-slash-to-dir( $dir );
  unless ( $d.IO.d ) {
    die "Can't find directory {$d}";
  }

  $_verbose = ! $silent;
  $_remove-symlinks = $remove-symlinks;

  process-sub-dir( $d, '',

    # Use different $proc functions depending on --any-place.
    $any-place ??
      -> $a, $b {
        # Calculate the whole list of <dir0> md5sums on first invocation.
        if ( ! %dir0-md5sums ) {
          process-sub-dir( $d0, '', -> $aa, $bb {
            my $md5 = file_md5_hex( $bb );
            %dir0-md5sums{ $md5 } = $bb;
          } );
        }

        my $md5 = file_md5_hex( $b );
        if ( %dir0-md5sums{ $md5 }:exists ) {
          if ( unlink-file( $b ) ) {
            say "{$b} -> remove ( {%dir0-md5sums{ $md5 }} )" if $_verbose;
          }
        }
      }
    !!
      -> $a, $b {
        # Compare the files in <dir0> and <dir>. Use different defaults for
        # both calculations to prevent removals if file_md5_hex() returns empty
        # value.
        if ( $a.IO.f ) {
          my $md5 = file_md5_hex( $b ) || '--';
          my $md5_0 = file_md5_hex( $a ) || '---';
          if ( $md5_0 eq $md5 ) {
            if ( unlink-file( $b ) ) {
              say "{$b} -> remove" if $_verbose;
            }
          }
        }
      }

  );
}

# NB: This function is recursive and iterates through <dir> (or <dir0> in case
# of initial collection of md5sums for --any-place option).
# $proc is how each leaf file should be processed (takes two full file names:
# first from <dir0> and second from <dir> ).
sub process-sub-dir( Str $root-dir, $sub-dir, Code $proc ) {
  my $dir = $root-dir ~ $sub-dir;
  for $dir.IO.dir -> $fn0 {
    my $fn = $fn0.basename;
    my $fns = $sub-dir ?? $sub-dir ~ '/' ~ $fn !! $fn;
    my $fnf = $root-dir ~ $fns;
    my $fnf0 = $d0 ~ $fns; # NB: Use of a global !!!

    if ( $fnf.IO.d ) {
      # Only perform dir symlink checks and skips if it's not an invocation to
      # collect md5sums from <dir0> for --any-place option
      # TODO: replace '$root-dir ne $d0' with something more reliable 
      if ( $root-dir ne $d0 ) {
        # Skip dirs that are inside <dir0> when $root-dir isn't <dir0>
        if ( is-dir-under-dir0( $fnf ) ) {
          say "{$fnf} -> skip (because it's under source dir {$d0})"
            if ( $_verbose );
          next;
        }
        # Skip going into symlinked dirs unless --remove-symlinks option
        if ( $fnf.IO.l && ! $_remove-symlinks ) {
          say "{$fnf} -> skip symlink"
            if ( $_verbose );
          next;
        }
      }
      process-sub-dir( $root-dir, $fns, $proc );
    }

    elsif ( $fnf.IO.f ) {
      &$proc( $fnf0, $fnf );
    }
  }

  # Only delete current dir if it turns out to be empty after the removals
  # in $proc .
  # Only do that if $dir doesn't contain $d0 to prevent deleting empty
  # directories from <dir0> in --any-place $proc initialization run.
  if ( $dir !~~ m:i/ ^ $d0 / ) {
    if ( $dir.IO.l ) {
      # Remove symlinks to empty directories if they don't contain files and
      # --remove-symlinks is specified.
      if ( $_remove-symlinks && ! $dir.IO.dir.elems ) {
        my $linked-dir = $dir.IO.resolve(:completely).Str;
        unlink( $dir );
        say "{$dir} -> remove symlink to empty dir" if $_verbose;
        # Also delete the linked dir
        $dir = $linked-dir;
      }
    }
    my @deleted-dirs = rmdir( $dir );
    if ( $dir eq any( @deleted-dirs ) ) {
      say "{$dir} -> rmdir" if $_verbose;
    }
  }
}

# Perform additional checks before calling unlink() on any files. This is
# supposed to prevent deleting the files under <dir0> if they are symlinked
# to target directory or --any-place option is used with two overlapping
# directories. Also don't remove any symlinks if no --remove-symlinks option
# is specified.
sub unlink-file ( Str $fnf ) {
  if ( is-dir-under-dir0( $fnf ) ) {
    say "{$fnf} -> skip (because it's under source dir {$d0})"
      if ( $_verbose );
    return False;
  }
  if ( ! $_remove-symlinks && $fnf.IO.l ) {
    say "{$fnf} -> skip (file symlink)" if ( $_verbose );
    return False;
  }
  unlink( $fnf );
  return True;
}

sub is-dir-under-dir0 ( Str $dir ) {
  if ( $dir.IO.resolve(:completely).Str ~~ m:i/ $d0-full / ) {
    return True;
  }
  return False;
}

sub append-slash-to-dir ( Str $dir ) returns Str {
  my $d = $dir;
  if ( $d !~~ m/ \/ \s* $ / ) {
    $d ~= '/';
  }
  return $d;
}

# Since Digest::MD5 is not in the default rakubrew package, let's use external
# command.
sub file_md5_hex ( Str $file-name ) returns Str {
  my $md5sum = '/usr/bin/md5sum';
  if ( ! $md5sum.IO.e ) {
    die "The script depends on {$md5sum} system utility being installed.";
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
