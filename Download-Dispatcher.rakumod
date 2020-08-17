=begin pod

=head1 Download-Dispatcher.rakumod

Classes that help to create HTTP download queue crontab scripts.

An example of crontab script that could be used with this module:

=begin code
use lib <.>;
use Download-Dispatcher;

my $dl = Download.new: :limit-rate(160000) :download-dir('.');
my $d = Dispatcher.new :d($dl) :dispatcher-dir('.');

$d.url-converters<url> = {
  my $u = @_[0]; $u ~~ s/ \? .+ $ //; $u ~ '';
}
$d.url-converters<file-name> = {
  # convert URL into output file name as the chars between last slash and
  # subsequent ? or end of string
  my $u = @_[0]; my $m = $u ~~ m/ ^ .+ \/ (.+?) (\?|$) /; '' ~ $m[0];
}

$d.main();
=end code

=head2 TODO

=item Implement import file to prevent race conditions on 'to-download'
=item Run only within certain timespan each day
=item Additional options for `wget`

=head2 AUTHOR

Victor Ichalov <ichalov@gmail.com>, 2020

=end pod

unit module Download-Dispatcher;

my %dispatcher-storage = (
  to-download => 'download.txt',
  downloading => 'downloading.txt',
  complete => 'complete.txt',
);

# NB: depends on wget, screen and ps.
class Download is export {

  has $.download-dir is rw = '.';

  has $.limit-rate is rw; # in bytes

  has $!wget = '/usr/bin/wget';

  method start-download( $url, $file-name ) {
    my @cmd = ( '/usr/bin/screen', '-d -m', $!wget, '-c' );
    if ( $.limit-rate ) {
      @cmd.push: "--limit-rate={$.limit-rate}";
    }
    @cmd.push: "'{$url}'", "-O {$.download-dir}/{$file-name}";
    shell( @cmd.join: ' ' );
  }

  method wget-process-exists( Str $url ) returns Bool {
    my $proc = run( '/bin/ps', 'auxww', :out );
    my $out = $proc.out.slurp;

    if ( $out ~~ m:i/ \W wget \W .+? $url / ) {
      return True;
    }
    return False;
  }

  method get-file-size( Str $url ) returns Int {
    my $proc = run(
      $!wget, '--server-response', '--spider', $url, :err
    );
    my $out = $proc.err.slurp;
    if ( my $match = $out ~~ m:i/content\-length\s*\:\s*(\d+)/ ) {
      return $match[0].Int;
    }
    return 0;
  }
}

# NB: main() is the entry point, make other methods private.
class Dispatcher is export {

  has $.dispatcher-dir is rw = '.';

  has %.url-converters is rw = (
    url => { @_[0] },
      # TODO: replace with more generic
    file-name => { my $u = @_[0]; $u ~~ s/^.+\///; $u ~ ''; },
  );

  has Download $.d is rw;

  method main() {
    my @cur = self.get-current-download();
    if ( @cur ) {
      self.check-restart-download( @cur[0], @cur[1].Int );
    }
    elsif ( my $url = self.next-download() ) {
      self.schedule-download( $url );
    }
  }

  method add-download( Str $url ) {
    my $fn = self.control-file( 'to-download' );
    if ( $fn.IO.e ) {
      my $dispatcher-downloads = $fn.IO.slurp;
      for $dispatcher-downloads.split("\n") -> $u {
        if ( $url.lc eq $u.lc ) {
          self.post-error-message( "URL already scheduled" );
        }
      }
    }
    spurt $fn, "{$url}\n", :append;
  }

  method next-download() {
    my $fn = self.control-file( 'to-download' );
    if ( $fn.IO.e ) {
      for $fn.IO.lines -> $url {
        return $url if ( $url && $url !~~ m/^\s*\#/ );
      }
    }
    self.post-log-message( "Nothing to do" );
    return;
  }

  method schedule-download( Str $url0 ) {
    unless ( $.d ) {
      $.d = Download.new;
    }
    my $url = %.url-converters<url>( $url0 );

    my $target-size = $.d.get-file-size( $url );

    my $fn0 = self.control-file( 'to-download' );
    my $fn = self.control-file( 'downloading' );

    if ( ! $fn0.IO.e ) {
      self.post-error-message( "Can't find file {$fn0}" );
      return;
    }

    if ( $fn.IO.e && self.get-current-download() ) {
      self.post-error-message( "{$fn} is not empty" );
      return;
    }

    # TODO: Maybe start download immediately and don't wait for
    # check-restart-download()

    spurt $fn, "{$url0}\t{$target-size}\n";

    # TODO: Maybe need a more robust solution for removing lines from source
    my @lines = $fn0.IO.lines.grep( { ! /$url0/ } );
    spurt $fn0, @lines.join("\n") ~ "\n";

    self.post-log-message( "Scheduled {$url0} for download" );
  }

  method get-current-download() {
    my $fn = self.control-file( 'downloading' );
    if ( $fn.IO.e ) {
      for $fn.IO.lines -> $line {
        my @cols = $line.split( "\t" );
        if ( @cols.elems > 1 ) {
          # it's supposed to be formatted like: URL, size (bytes)
          return @cols;
        }
      }
    }
    return Empty;
  }

  method check-restart-download( Str $url0, Int $size ) {
    unless ( $.d ) {
      $.d = Download.new;
    }

    my $url = %.url-converters<url>( $url0 );
    my $file-name = %.url-converters<file-name>( $url0 );

    if ( ! $.d.wget-process-exists( $url ) ) {
      my $target_fn = $.d.download-dir ~ '/' ~ $file-name;
      if ( $target_fn.IO.e ) {
        my $d_size = $target_fn.IO.s;
        if ( $d_size >= $size ) {
          if ( $d_size > $size ) {
            self.post-log-message( "URL {$url0} download size exceeds estimate "
                                 ~ "({$d_size} vs. {$size}). Stopped "
                                 ~ "downloading." );
          }
          else {
            self.post-log-message( "URL {$url0} finished." );
          }
          self.finalize-download( $url0 );
        }
        else {
          self.post-log-message( "Restarted incomplete URL download: {$url0}" );
          $.d.start-download( $url, $file-name );
        }
      }
      else {
        self.post-log-message( "Start missing URL download: {$url0}" );
        $.d.start-download( $url, $file-name );
      }
    }
    else {
      self.post-log-message( "URL download underway: {$url0}" );
    }
  }

  method finalize-download( Str $url ) {
    my $fn0 = self.control-file( 'downloading' );
    my $fn = self.control-file( 'complete' );

    if ( ! $fn0.IO.e ) {
      self.post-error-message( "Can't find file {$fn0}" );
      return;
    }

    # NB: 'downloading' file only supposed to have only one line
    my $l = $fn0.IO.lines[0];
    if ( $l ~~ m:i/ $url / ) {
      spurt $fn, $l ~ "\n", :append;
      spurt $fn0, '';
    }
    else {
      self.post-error-message( "First line of {$fn0} doesn't contain required "
                             ~ "URL {$url}" );
    }
  }

  method control-file ( Str $fid ) {
    return $.dispatcher-dir ~ '/' ~ %dispatcher-storage{ $fid };
  }

  method post-log-message( Str $msg ) {
    say DateTime.now( formatter => { .yyyy-mm-dd ~ ' ' ~ .hh-mm-ss } ) ~ ' -- '
      ~ $msg;
  }

  method post-error-message( Str $msg ) {
    self.post-log-message( 'ERROR: ' ~ $msg );
    die $msg;
  }
}

