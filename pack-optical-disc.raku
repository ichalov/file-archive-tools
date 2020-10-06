#`(
  pack-optical-disc.raku - given a plain source directory of [large] files,
    tries to find combinations of those files that fit best on optical disks of
    various sizes.

  TODO:
    Progress bar during calculation
    Output correspondent mkisofs commands
    Get rid of global variables
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

  Tries to fit files from <src> on optical disks of various sizes:
  { ' ' x 4 ~ %container-size-limits.keys
        .sort( { %container-size-limits{$^a} <=> %container-size-limits{$^b} } )
        .join(', '); }
  Prints report with best fitting combinations of files on STDOUT.
  <src> can be a directory or a file with `ls -l` output of the source directory
  (or a dash for taking `ls -l` output from STDIN).
  EOT

sub USAGE() {
  say $*USAGE;
  say $script-description;
}


# Global storages for file lists
my @file-names = Empty;
my @file-sizes = Empty;
my %file-sets = Empty;

my $_speedups = False;

sub MAIN ( Str $src, Bool :$speedups, Str :$min-size ) {

  $_speedups = $speedups;

  my $start-time = DateTime.now();

  my $min-size-bytes = $min-size ?? parse-file-size( $min-size ) !! 0;

  my %src-files = Empty;
  if ( $src.IO.d ) {
    for $src.IO.dir -> $file {
      next unless $file.IO.f;
      my $size = $file.IO.s;
      %src-files{ $file.basename } = $size;
    }
  }
  elsif ( $src.IO.f ) {
    %src-files = lsl-to-file-sizes( $src.IO.slurp );
  }
  elsif ( $src eq '-' ) {
    %src-files = lsl-to-file-sizes( slurp );
  }
  else {
    die "Can't parse source directory: {$src}";
  }

  unless %src-files {
    die "Can't find any files in the source directory {$src}";
  }

  for %src-files.kv -> $file, $size {
    state Int $counter;
    next if $size < $min-size-bytes;
    @file-names[++$counter] = $file;
    @file-sizes[$counter] = $size;
  }

  # order the initial @tail as larger files first
  my @init-tail = @file-names.keys.grep({ $_ })
                    .sort: { @file-sizes[$^b] <=> @file-sizes[$^a] };

  # DIAG
#  say @init-tail.map({$^a ~ ' ' ~ @file-names[$^a]}).gist;
#  exit;

  check-combination( Empty, @init-tail, 0 );

  report-best-combinations();

  say ( DateTime.now() - $start-time ).round(0.1) ~ ' seconds elapsed';
}

# NB: This recursive function relies on global @file-sizes and
# %container-size-limits, writes in %file-sets.
sub check-combination( @base, @tail, $base-size ) {
  TAIL: for @tail -> $base-add {
    my $new-size = $base-size + @file-sizes[$base-add];
    for %container-size-limits.keys
          .sort({%container-size-limits{$^a}})
        -> $disc
    {
      my $limit = %container-size-limits{$disc};
      if ( $base-size <= $limit && $new-size > $limit ) {
#        say $disc ~ '|' ~ @base.join: '|'; # DIAG

        # TODO: Decrease %file-sets size by filtering out candidates that have
        # little chance of getting into final report ($limit - $base-size is too
        # big).
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
          .map({ $_.Numeric !~~ Failure ?? '  ' ~ @file-names[$_] !! $_ })
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

sub lsl-to-file-sizes( Str $lsl ) {
  my %file-sizes = Empty;

  # TODO: Find a better way of handling spaces in dates and file names
  my @m = $lsl ~~ m:g/ ^^\- [.+?\s+]**4 (\d+) \s+ [.+?\s+]**3 (.+?) $$ /;

  for @m -> $m {
    %file-sizes{ $m[1] } = $m[0];
  }

  return %file-sizes;
}

sub format-int( Int $n0 ) {
  my $n = $n0.flip;
  $n ~~ s:g/(\d ** 3)/$0,/;
  $n ~~ s/\,$//;
  return $n.flip;
}

