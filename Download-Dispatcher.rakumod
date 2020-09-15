=begin pod

=head1 Download-Dispatcher.rakumod

Classes that help to create HTTP download queue crontab scripts.

An example of crontab script that could be used with this module:

=begin code
use lib <.>;
use Download-Dispatcher;

my $dl = Wget-Download.new: :limit-rate(160000) :download-dir('.');
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

=item Run only within certain timespan each day
=item Additional options for `wget`

=head2 AUTHOR

Victor Ichalov <ichalov@gmail.com>, 2020

=end pod

unit module Download-Dispatcher;

my %dispatcher-storage = (
  incoming => 'incoming.txt',
  work-queue => 'download.txt',
  downloading => 'downloading.txt',
  complete => 'complete.txt',
);

role Download is export {

  has $.download-dir is rw = '.';

  has %.file-params-cache = Empty;

  method start-download( Str $url, Str $file-name ) {
    die "Abstract method called";
  }

  method download-process-exists( Str $url ) returns Bool {
    die "Abstract method called";
  }

  method get-file-params( Str $url ) returns Hash {
    die "Abstract method called";
  }

  method get-file-params-cached( Str $url ) returns Hash {
    if ( %.file-params-cache{ $url }:exists ) {
      return %.file-params-cache{ $url };
    }
    else {
      my %file-params = self.get-file-params( $url );
      %.file-params-cache{ $url } = %file-params;
      return %file-params;
    }
  }
}

# NB: depends on wget, screen and ps.
class Wget-Download does Download is export {

  has $.limit-rate is rw; # in bytes

  has $!wget = '/usr/bin/wget';

  method start-download( $url, $file-name ) {
    my @cmd = ( '/usr/bin/screen', '-d', '-m', $!wget, '-c' );
    if ( $.limit-rate ) {
      @cmd.push: "--limit-rate={$.limit-rate}";
    }
    @cmd.push: $url, '-O', "{$.download-dir}/{$file-name}";
    run( @cmd );
  }

  method download-process-exists( Str $url ) returns Bool {
    my $proc = run( '/bin/ps', 'auxww', :out );
    my $out = $proc.out.slurp;

    if ( $out ~~ m:i/ \W wget \W .+? $url / ) {
      return True;
    }
    return False;
  }

  method get-file-params( Str $url ) returns Hash {
    my $proc = run(
      $!wget, '--server-response', '--spider', $url, :err
    );
    my $out = $proc.err.slurp;
    my %ret = Empty;
    if ( $out ~~ m:i/content\-length\s*\:\s*(\d+)/ ) {
      %ret<size-bytes> = $/[0].Int;
    }
    # NB: The following may in theory be used for shell injection attack but
    # is handled by using run() instead of shell() in self.start-download() .
    if ( $out ~~ m:i/content\-disposition\s*\:.+?filename\=\"(.+?)\"/ ) {
      %ret<file-name> = $/[0].Str;
    }
    # TODO: Handle RFC 5987 reqs better
    if ( $out ~~ m:i/content\-disposition\s*\:.+?filename\*?\=.+\'(.+?)$$/ ) {
      %ret<file-name> = $/[0].Str;
    }
    return %ret;
  }
}

# NB: depends on youtube-dl, screen and ps.
class YT-DL-Download does Download is export {

  has $.limit-rate is rw; # in bytes

  has $.youtube-dl = '/usr/bin/youtube-dl';

  has $!format-id;

  method start-download( $url, $file-name ) {
    my @cmd = ( '/usr/bin/screen', '-d', '-m', $.youtube-dl, '--no-call-home' );
    if ( $.limit-rate ) {
      @cmd.push: '-r', $.limit-rate;
    }
    # Don't use format_id because it prevents merge (the output won't have audio
    # stream included).
#    if ( $!format-id ) {
#      @cmd.push: '-f', $!format-id;
#    }
    @cmd.push: $url;
    run( @cmd );
  }

  method download-process-exists( Str $url ) returns Bool {
    my $proc = run( '/bin/ps', 'auxww', :out );
    my $out = $proc.out.slurp;

    if ( $out ~~ m:i/ \W youtube\-dl \W .+? $url / ) {
      return True;
    }
    return False;
  }

  method get-file-params( Str $url ) returns Hash {

    use JSON::Tiny;

    if ( $.download-dir.IO.d ) {
      chdir( $.download-dir );
    }
    else {
      die( "Can't open download dir: {$.download-dir}" );
    }

    my $proc = run(
      $.youtube-dl, '--no-call-home', '--write-info-json', '--skip-download',
      $url, :out
    );
    my $out = $proc.out.slurp;
    my %ret = Empty;
    my $json-file-name;
    if ( $out ~~ m:i/
      ^^ '[info] Writing video description metadata as JSON to: ' \s* (.+) $$
    / ) {
      $json-file-name = $/[0].Str;
      my $json = from-json( $json-file-name.IO.slurp );

      # Calculate approximate resulting file size as the sum of last sized 720p
      # video and audio with acodec as in header. If the video of choice has
      # both vcodec and acodec, then take its size without adding audio.
      # NB: youtube-dl names the file with .part suffix while downloading and
      # renames it into target only after completion. So Dispatcher is not able
      # to identify abandoned download by seeing incomplete file. It will just
      # start youtube-dl with normal command which continues incomplete .part
      # file by default. But both 'file-name' and 'size-bytes' attributes are
      # required to identify the file is complete and doesn't need restart.
      # TODO: Need a more reliable means of getting the resulting file size
      # before it's downloaded.
      my ( $video-size, $audio-size, $compound-size ) = 0, 0, 0;
      sub set-ret ( Hash $fmt ) {
        %ret<file-name> = $json-file-name;
        %ret<file-name> ~~ s:i/\.info\.json\s*$/.$fmt<ext>/;
        $!format-id = $fmt<format_id>;
      }
      for |$json<formats> -> $fmt {
        if (
          $fmt<vcodec> eq $json<vcodec> && $fmt<acodec> eq $json<acodec>
          &&
          $fmt<filesize> && $fmt<filesize> > $compound-size
        ) {
          $compound-size = $fmt<filesize>;
          set-ret( $fmt );
        }
        if (
          $fmt<format_note> eq '720p' && $fmt<acodec> eq 'none'
          &&
          $fmt<filesize>
        ) {
          $video-size = $fmt<filesize>;
          unless ( $compound-size ) {
            set-ret( $fmt );
          }
        }
        if (
          $fmt<acodec> eq $json<acodec> && $fmt<vcodec> eq 'none'
          &&
          $fmt<filesize> && $fmt<filesize> > $audio-size
        ) {
          $audio-size = $fmt<filesize>;
        }
      }
      %ret<size-bytes> = $compound-size || $video-size + $audio-size;
    }
    if ( $json-file-name && $json-file-name.IO.f ) {
      $json-file-name.IO.unlink;
    }
    return %ret;
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
    self.copy-from-incoming();
    my @cur = self.get-current-download();
    if ( @cur ) {
      self.check-restart-download( @cur[0], @cur[1].Int );
    }
    elsif ( my $url = self.next-download() ) {
      self.schedule-download( $url );
    }
  }

  method copy-from-incoming() {
    my $fn = self.control-file( 'incoming' );
    my $cont = '';
    if ( $fn.IO.e ) {
      for $fn.IO.lines -> $l {
        self.post-log-message( "Queueing incoming download: {$l}" );
        $cont ~= $l ~ "\n";
      }
    }
    spurt self.control-file( 'work-queue' ), $cont, :append;
    spurt $fn, '';
  }

  method add-download( Str $url ) {
    my $fn = self.control-file( 'work-queue' );
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
    my $fn = self.control-file( 'work-queue' );
    if ( $fn.IO.e ) {
      for $fn.IO.lines -> $url {
        return $url if ( $url && $url !~~ m/^\s*\#/ );
      }
    }
    self.post-log-message( "Nothing to do" );
    return;
  }

  method schedule-download( Str $url0 ) {
    my $url = %.url-converters<url>( $url0 );

    my $target-size = $.d.get-file-params-cached( $url )<size-bytes>;

    my $fn0 = self.control-file( 'work-queue' );
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

    my $url = %.url-converters<url>( $url0 );

    # NB: It's important to have consistent file name over time, so either
    # get it from downloader or calculate from URL.
    my $file-name = $.d.get-file-params-cached( $url )<file-name>
                 // %.url-converters<file-name>( $url0 );

    if ( ! $.d.download-process-exists( $url ) ) {
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

