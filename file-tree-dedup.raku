#`(
  file-tree-dedup.raku
  Author: <ichalov@gmail.com>, 2020-08-12

  TODO:
    Implement --any-place
    Implement --verbose mode
    Handle symlinks
    Protect against <dir> = <dir0> or one directory inside another
    Use Digest::MD5 to improve speed on small files

)

my $script-description = Q:c:to/EOT/;

  This script removes files under <dir> that also exist under <dir0>.
  It handles <dir> recursively and deletes files only if their copies are placed
  in exactly same subdir of <dir0>. Option --any-place relaxes the requirement
  of the same position and deletes file from <dir> if the file with same md5sum
  is found anywhere under <dir0>.
  EOT

sub USAGE() {
  say $*USAGE;
  say $script-description;
}

# Global storage for <dir0>
my $d0;


sub MAIN( Str $dir0, Str $dir, Bool :$any-place ) {
  $d0 = append-slash-to-dir( $dir0 );
  unless ( $d0.IO.d ) {
    die "Can't find directory {$d0}";
  }

  my $d = append-slash-to-dir( $dir );
  unless ( $d.IO.d ) {
    die "Can't find directory {$d}";
  }

  process-sub-dir( $d );
}

# NB: This function is recursive and iterates through <dir>
sub process-sub-dir( Str $root-dir, $sub-dir = '' ) {
  my $dir = $root-dir ~ $sub-dir;
  for $dir.IO.dir -> $fn0 {
    my $fn = $fn0.basename;
    my $fns = $sub-dir ?? $sub-dir ~ '/' ~ $fn !! $fn;
    my $fnf = $root-dir ~ $fns;
    my $fnf0 = $d0 ~ $fns;
    if ( $fnf.IO.d ) {
      process-sub-dir( $root-dir, $fns );
    }
    elsif ( $fnf.IO.f && $fnf0.IO.f ) {
      # Compare the files in <dir0> and <dir>. Use different defaults for
      # both calculations to prevent removals if file_md5_hex() returns empty
      # value.
      my $md5 = file_md5_hex( $fnf ) || '--';
      my $md5_0 = file_md5_hex( $fnf0 ) || '---';
      if ( $md5_0 eq $md5 ) {
        say "{$fnf} -> remove";
        unlink( $fnf );
      }
    }
  }

  # Only delete current dir if it turns out to be empty after the above removals
  rmdir( $dir );
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
