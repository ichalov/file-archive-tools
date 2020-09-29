#`(
  pack-optical-disc.raku - given a plain source directory of [large] files,
    tries to find combinations of those files that fit best on optical disks of
    various sizes.

  TODO:
    Progress bar during calculation
    Output correspondent mkisofs commands
    Feed from `ls -l` instead of live directory
    Add parallelization

  Author: Victor Ichalov <ichalov@gmail.com>, 2020
)

# Sizes of containers (optical discs) to fit the files on (in bytes):
my %container-size-limits = (
  'dvd4.5'  => 4700000000,
  'dvd8.5'  => 8500000000,
  'bd-r-25' => 25000000000,
);
my $max-container-size = max( %container-size-limits.values );

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

sub MAIN ( Str $src-dir, Bool :$speedups, Str :$min-size ) {

  my $start-time = DateTime.now();

  my $min-size-bytes = $min-size ?? parse-file-size( $min-size ) !! 0;

  my @file-names = Empty;
  my @file-sizes = Empty;

  # TODO: Perform check that number of files fits into native type. Or implement
  # automatic choice of the type.
  my int8 $counter = 0;
  for $src-dir.IO.dir -> $file {
    next unless $file.IO.f;
    next if $file.IO.s < $min-size-bytes;
    @file-names[++$counter] = $file.basename;
    @file-sizes[$counter] = $file.IO.s;
  }

  my %file-sets = calculate-combination-sizes(
    @file-names, @file-sizes, speedups => $speedups
  );

  report-best-combinations( %file-sets, @file-names );

  say ( DateTime.now() - $start-time ).round(0.1) ~ ' seconds elapsed';
}

sub calculate-combination-sizes( @file-names, @file-sizes, *%options ) {

  my $speedups = %options<speedups>;

  my %file-sets = Empty;

  # order the initial combination as larger files first
  my @init = @file-names.keys.grep({ $_ })
    .sort: { @file-sizes[$^b] <=> @file-sizes[$^a] };

  my @combination-stack = Empty;
  @combination-stack.push: [ (), @init, 0 ];

  my $stack-pos = 0;
  while ( my $item = @combination-stack[ $stack-pos++ ] // False ) {
   # TODO: Find a better method of storing items in stack (e.g. w/o using .flat)
   my @base = $item[0].flat;
   my @tail = $item[1].flat;
   my $base-size = $item[2];

   TAIL: for @tail -> $base-add {
    my $new-size = $base-size + @file-sizes[$base-add];
    for %container-size-limits.keys
          .sort({%container-size-limits{$^a}})
        -> $disc
    {
      my $limit = %container-size-limits{$disc};
      if ( $base-size <= $limit && $new-size > $limit ) {
#        say $disc ~ '|' ~ @base.join: '|'; # DIAG

        %file-sets{$disc ~ '|' ~ @base.sort.join: '|'} = $base-size;

        # Don't check any further combinations if the current $limit is achieved
        # with more than 2 files. Only make this shortcut if --speedups option
        # is specified.
        if ( $speedups && @base.elems >= 2 ) {
          next TAIL;
        }
      }
    }

    # Don't try additional files over @base + $base-add if $new-size is already
    # bigger than the largest container size
    if ( $new-size > $max-container-size ) {
      next;
    }

    my @next-tail = @tail.grep: { $_ != $base-add };
    @combination-stack.push: [ ( @base, $base-add ).flat, @next-tail, $new-size ];
   }
  }

  return %file-sets;
}

# NB: reads from global %file-sets, prints on STDOUT
sub report-best-combinations( %file-sets, @file-names, Int $report-items = 10 ) {
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
          .map({ $_.Numeric !~~ Failure ?? '  ' ~ @file-names[$_] !! $_})
          .join("\n"),
        "\n = ", format-int( %file-sets{$file-set} ), ' (',
        format-int( $container-size - %file-sets{$file-set} ), " remaining)\n";
    last if (++$count > $report-items);
  }
}

sub get-container-name-from-file-set ( Str $file-set ) {
  my $m = $file-set ~~ m/ ^ (.+?) \| /;
  $m[0];
}

sub parse-file-size( Str $size ) {
  if ( $size ~~ m:i/ ^ \s* (\d+) (k|m|g)? \s* $ / ) {
    my $num = $/[0];
    my $mult = 0;
    given $/[1] // '' {
      when .lc eq 'k' { $mult = 10 }
      when .lc eq 'm' { $mult = 20 }
      when .lc eq 'g' { $mult = 30 }
    }
    return $num * 2 ** $mult;
  }
  else {
    die "Can't parse file size '{$size}', should be an integer optionally "
      ~ "followed by K, M or G, e.g.: 300k or 2G or 200000 (bytes)";
  }
}

sub format-int( Int $n0 ) {
  my $n = $n0.flip;
  $n ~~ s:g/(\d ** 3)/$0,/;
  $n ~~ s/\,$//;
  return $n.flip;
}

