#`(
  file-tree-dedup.raku
  Author: <ichalov@gmail.com>, 2020-08-12
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

sub MAIN( Str $dir0, Str $dir, Bool :$any-place ) {

}

