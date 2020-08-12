#`(
  file-tree-dedup.raku
  Author: <ichalov@gmail.com>, 2020-08-12

  TODO:
    Think whether symlinks need handling
    Protect against <dir> = <dir0> or one directory inside another
    Use Digest::MD5 to improve speed on small files

)

my $script-description = Q:c:to/EOT/;

  This script removes files under <dir> that also exist under <dir0>.
  It handles <dir> recursively and deletes files only if their copies are placed
  in exactly same subdir of <dir0>. Option --any-place relaxes the requirement
  of the same position and deletes file from <dir> if the file with same md5sum
  is found anywhere under <dir0>.
  It only prints what it does if --verbose option is specified.
  EOT

sub USAGE() {
  say $*USAGE;
  say $script-description;
}

# Global storage for <dir0>
my $d0;

# Global storage for --verbose option presence
my Bool $_verbose;

# Global hash for md5sums of files in <dir0> in case of --any-place.
# TODO: Better re-make using state variable, but difficult.
my %dir0-md5sums = Empty;


sub MAIN( Str $dir0, Str $dir, Bool :$any-place, Bool :$verbose ) {
  $d0 = append-slash-to-dir( $dir0 );
  unless ( $d0.IO.d ) {
    die "Can't find directory {$d0}";
  }

  my $d = append-slash-to-dir( $dir );
  unless ( $d.IO.d ) {
    die "Can't find directory {$d}";
  }

  $_verbose = $verbose;

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
          say "{$b} -> remove ( {%dir0-md5sums{ $md5 }} )" if $_verbose;
          unlink( $b );
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
            say "{$b} -> remove" if $_verbose;
            unlink( $b );
          }
        }
      }

  );
}

# NB: This function is recursive and iterates through <dir>
# $proc is how each leaf file should be processed (takes two full file names:
# first from <dir0> and second from <dir>.
sub process-sub-dir( Str $root-dir, $sub-dir, Code $proc ) {
  my $dir = $root-dir ~ $sub-dir;
  for $dir.IO.dir -> $fn0 {
    my $fn = $fn0.basename;
    my $fns = $sub-dir ?? $sub-dir ~ '/' ~ $fn !! $fn;
    my $fnf = $root-dir ~ $fns;
    my $fnf0 = $d0 ~ $fns; # NB: Use of a global !!!
    if ( $fnf.IO.d ) {
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
    my @deleted-dirs = rmdir( $dir );
    if ( $dir eq any( @deleted-dirs ) ) {
      say "{$dir} -> rmdir" if $_verbose;
    }
  }
}

sub append-slash-to-dir( Str $dir ) returns Str {
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
