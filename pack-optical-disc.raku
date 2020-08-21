#`(
  pack-optical-disc.raku - given a plain source directory of [large] files,
    tries to find combinations of those files that fit best on optical disks of
    various sizes.

  TODO:
    Progress bar during calculation
    Feed from `ls -l` instead of live directory
    Get rid of global variables
    Add parallelization
    Optional low limit on the file size to be included (to improve speed)

  Author: Victor Ichalov <ichalov@gmail.com>, 2020
)

# Sizes of containers (optical discs) to fit the files on (in bytes):
my %container-size-limits = (
  'dvd4.5'  => 4700000000,
  'dvd8.5'  => 8500000000,
  'bd-r-25' => 25000000000,
);
my $max-container-size = max( %container-size-limits.values );
my $limit-before-max = %container-size-limits.values.sort[*-2];

my $script-description = Q:c:to/EOT/;

  Tries to fit files from <src-dir> on optical disks of various sizes:
  { ' ' x 4 ~ %container-size-limits.keys
        .sort( { %container-size-limits{$^a} <=> %container-size-limits{$^b} } )
        .join(', '); }
  Prints report with best fitting combinations of files on STDOUT.
  EOT

sub USAGE() {
  say $*USAGE;
  say $script-description;
}


# Global storages for file lists
my %candidates = Empty;
my %file-names = Empty;
my %file-sets = Empty;

# Global storages for a speedup feature that shortcuts file combinations that
# are a different ordering of already checked but with fixed lead of the size
# no less than the second biggest container.
my $cur-limit;
my $base-at-limit;
my %visited-combinations;

my $_speedups = False;

sub MAIN ( Str $src-dir, Bool :$speedups ) {

  $_speedups = $speedups;

  my $start-time = DateTime.now();

  for $src-dir.IO.dir -> $file {
    state $counter;
    next unless $file.IO.f;
    %file-names{++$counter} = $file.basename;
    %candidates{$counter} = $file.IO.s;
  }

  # order the initial @tail as larger files first
  my @init-tail = %candidates.keys.sort:
                    { %candidates{$^b} <=> %candidates{$^a} };

  # DIAG
#  say @init-tail.map({$^a ~ ' ' ~ %file-names{$^a}}).gist;
#  exit;

  check-combination( Empty, @init-tail, 0 );

  report-best-combinations();

  say ( DateTime.now() - $start-time ).round(0.1) ~ ' seconds elapsed';
}

# NB: This recursive function relies on global %candidates and
# %container-size-limits, writes in %file-sets.
sub check-combination( @base, @tail, $base-size ) {
  TAIL: for @tail -> $base-add {
    my $new-size = $base-size + %candidates{$base-add};
    for %container-size-limits.keys
          .sort({%container-size-limits{$^a}})
        -> $disc
    {
      my $limit = %container-size-limits{$disc};
      if ( $base-size <= $limit && $new-size > $limit ) {
#        say $disc ~ '|' ~ @base.join: '|'; # DIAG

        # Save the combination of files that covers (exceeds) the disc
        # of the biggest size not yet filled. To be used in the speedup
        # shortcutting file combinations that are a different ordering of
        # already checked.
        if ( $limit < $max-container-size ) {
           $cur-limit = $limit;
           $base-at-limit = ( @base, $base-add ).flat.join('|');
        }

        %file-sets{$disc ~ '|' ~ @base.sort.join: '|'} = $base-size;

        # Don't check any further combinations if the current $limit is achieved
        # with more than 2 files. Only make this shortcut if --speedups option
        # is specified.
        if ( $_speedups && @base.elems >= 2 ) {
          next TAIL;
        }
      }
    }

    # Don't try additional files over @base + $base-add if $new-size is already
    # bigger than the largest container size
    if ( $new-size > $max-container-size ) {
      next;
    }

    # Make the script run faster by skipping combinations of files that are
    # a different ordering of already checked. The comparison only applies to
    # "tail" - the files that go to the space of the largest disc that is
    # addition over the second biggest disc.
    # It produces partial result (different to the one without shortcuts).
    if ( $_speedups && $cur-limit == $limit-before-max ) {
      my $next-base = ( @base, $base-add ).flat.join: '|';
#      put 'next-base: ' ~ $next-base; # DIAG
      if ( my $m = $next-base ~~ m/ ^ $base-at-limit \| (.+) / ) {
        my $tail = $m[0];
        my $sorted-tail = $tail.split('|').sort.join('|');
#        put 'sorted-tail: ' ~ $sorted-tail; # DIAG
        if ( %visited-combinations{ $base-at-limit }{ $sorted-tail }:exists ) {
#          say "skipping " ~ $base-at-limit ~ '|' ~ $sorted-tail; # DIAG
          next;
        }
        else {
          %visited-combinations{ $base-at-limit }{ $sorted-tail } = 1;
        }
      }
    }

    my @next-tail = @tail.grep: { $_ != $base-add };
    check-combination( ( @base, $base-add ).flat, @next-tail, $new-size );
  }
}

# NB: reads from global %file-sets, prints on STDOUT
sub report-best-combinations( Int $report-items = 10 ) {
  for
    %file-sets.keys.sort:
      {
        %container-size-limits{ get-container-name-from-file-set( $^a ) }
        -
        %file-sets{$^a}
      }
    -> $file-set
  {
    state $count;
    my $container-size =
      %container-size-limits{ get-container-name-from-file-set( $file-set ) };
    put $file-set.split('|')
          .map({%file-names{$_} ?? '  ' ~ %file-names{$_} !! $_}).join("\n"),
        "\n = ", format-int( %file-sets{$file-set} ), ' (',
        format-int( $container-size - %file-sets{$file-set} ), " remaining)\n";
    last if (++$count > $report-items);
  }
}

sub get-container-name-from-file-set ( Str $file-set ) {
  my $m = $file-set ~~ m/ ^ (.+?) \| /;
  $m[0];
}

sub format-int( Int $n0 ) {
  my $n = $n0.flip;
  $n ~~ s:g/(\d ** 3)/$0,/;
  return $n.flip;
}
