=begin pod

=head1 Download-Dispatcher.rakumod

Classes that help to create HTTP download queue crontab scripts.

An example of crontab script that could be used with this module:

=begin code
use lib <.>;
use Download-Dispatcher;

my $dl = Wget-Download.new: :limit-rate(102400) :download-dir('.');
my $d = Dispatcher.new :downloader($dl) :dispatcher-dir('.');

$d.url-converters<url> = {
  my $u = @_[0]; $u ~~ s/ \? .+ $ //; $u ~ '';
}
$d.url-converters<file-name> = {
  # convert URL into output file name as the chars between last slash and
  # subsequent ? or end of string
  my $u = @_[0]; my $m = $u ~~ m/ ^ .+ \/ (.+?) (\?|$) /; '' ~ $m[0];
}
$d.download-allowed = sub {
 my $h = DateTime.now.hour;
 return ( $h >= 2 && $h < 8 );
}
$d.tag-descriptors = (
  'distributions' => %(
    'url-regexps' => ( rx:i/centos.+iso\s*$/, rx:i/debian.+iso\s*$/ ),
    'downloader' => Wget-Download.new( :limit-rate(153600) ),
    'subdir' => 'distr'
  ),
);

$d.main();
=end code

=head2 TODO

=item Add protection against missing system utilities (dependencies check).
=item Explore creating a wrapper over `wget` that renames file into target upon
successful completion similarly to `youtube-dl`. The script could be simplified
by removing file size tracking in this case.
=item Re-test $.take-next-download-wo-delay when no output file name could be
derived.
=item Try to perform merge in Download class
=item Re-check what happens on exceptions

=head2 AUTHOR

Victor Ichalov <ichalov@gmail.com>, 2020

=end pod

unit module Download-Dispatcher;

# NB: The main class in this module is Dispatcher. It's located towards bottom
# of the file because of the dependencies that have to enter earlier.

my %dispatcher-storage = (
  incoming => 'incoming.txt',
  work-queue => 'download.txt',
  downloading => 'downloading.txt',
  restart-counts => 'restart-counts.txt',
  failed => 'failed.txt',
  complete => 'complete.txt',
);

role Download is export {

  has $.download-dir is rw = '.';

  has @.additional-command-line-switches = Empty;
  has @!initial-command-line-switches = Empty;

  submethod TWEAK ( :@additional-command-line-switches ) {
    @!initial-command-line-switches = @additional-command-line-switches;
  }

  has %.file-params-cache = Empty;

  method start-download( Str $url, Str $file-name ) {
    die "Abstract method called";
  }

  method download-process-exists( Str $url ) returns Int {
    die "Abstract method called";
  }

  method get-file-params( Str $url ) returns Hash {
    die "Abstract method called";
  }

  method stop-download( $url ) {
    if ( my $pid = self.download-process-exists( $url ) ) {
      say "Killing pid: {$pid}";
      run( '/bin/kill', '-9', $pid );
    }
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

  method process-exists-regex( Str $cmd, Str $url ) returns Regex {
    return
      rx:i{ ^^ \s* \w+ \s+ (\d+) \N+? <!after screen\N+> \W $cmd \W .+? $url };
  }

  method merge-in-command-line-switches( @new-switches ) {
    for @new-switches -> $clsw {
      unless $clsw.trim eq any( |$.additional-command-line-switches ) {
        $.additional-command-line-switches.append: $clsw
      }
    }
  }

  method reset-additional-command-line-switches() {
    @.additional-command-line-switches = @!initial-command-line-switches;
  }

  method check-create-subdir( Str $subdir ) {
    my $dir = $.download-dir ~ '/' ~ $subdir;
    mkdir( $dir ) unless ( $dir.IO.d )
  }
}

# NB: depends on wget, screen and ps.
class Wget-Download does Download is export {

  has $.limit-rate is rw; # in bytes per second

  has $!wget = '/usr/bin/wget';

  method start-download( $url, $file-name ) {

    # NB: This is needed because other downloaders might chdir() somewhere else
    # before
    chdir( $.download-dir );

    my @cmd = ( '/usr/bin/screen', '-d', '-m', $!wget, '-c' );
    if ( $.limit-rate ) {
      @cmd.push: "--limit-rate={$.limit-rate}";
    }
    if ( @.additional-command-line-switches ) {
      @cmd.append: @.additional-command-line-switches;
    }
    @cmd.push: $url, '-O', "{$.download-dir}/{$file-name}";
    run( @cmd );
  }

  method download-process-exists( Str $url ) returns Int {
    my $proc = run( '/bin/ps', 'auxww', :out );
    my $out = $proc.out.slurp;

    if ( $out ~~ self.process-exists-regex( 'wget', $url ) ) {
      return $/[0].Int;
    }
    return 0;
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

  has $.limit-rate is rw; # in bytes per second

  has $.youtube-dl = '/usr/bin/youtube-dl';

  method start-download( $url, $file-name ) {

    # TODO: Make youtube-dl save into subdir without chdir()
    if ( $file-name ~~ m/ ^ (\w+) \/ / ) {
      my $dir = $.download-dir ~ '/' ~ $/[0].Str;
      if ( $dir.IO.d ) {
        chdir( $dir );
      }
    }
    else {
      if ( $.download-dir.IO.d ) {
        chdir( $.download-dir );
      }
    }

    my @cmd = ( '/usr/bin/screen', '-d', '-m', $.youtube-dl );
    if ( $.limit-rate ) {
      @cmd.push: '-r', $.limit-rate;
    }
    if ( @.additional-command-line-switches ) {
      @cmd.append: @.additional-command-line-switches;
    }
    @cmd.push: $url;
    run( @cmd );
  }

  method download-process-exists( Str $url ) returns Int {
    my $proc = run( '/bin/ps', 'auxww', :out );
    my $out = $proc.out.slurp;

    if ( $out ~~ self.process-exists-regex( 'youtube-dl', $url ) ) {
      return $/[0].Int;
    }
    return 0;
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
      $.youtube-dl, |@.additional-command-line-switches,
      '--write-info-json', '--skip-download', $url, :out
    );
    my $out = $proc.out.slurp;
    my %ret = Empty;
    my $json-file-name;
    if ( $out ~~ m:i/
      ^^ '[info] Writing video description metadata as JSON to: ' \s* (.+) $$
    / ) {
      $json-file-name = $/[0].Str;
      my $json = from-json( $json-file-name.IO.slurp );

      # NB: youtube-dl names the file with .part suffix while downloading and
      # renames it into target only after completion. So Dispatcher is not able
      # to identify abandoned download by seeing incomplete file. It will just
      # start youtube-dl with normal command which continues incomplete .part
      # file by default. The size is difficult to calculate and can be set to
      # zero because Dispatcher can identify the download completion by
      # appearance of the target file without .part suffix in the download
      # directory.
      my $ext;
      sub ext-by-format-id( $fmt0 ) {
        for |$json<formats> -> $fmt {
          if ( $fmt<format_id> eq $fmt0 ) {
            return $fmt<ext>;
          }
        }
        return '';
      }
      if ( $json<format_id> ~~ m/ ^ (\d+) \+ (\d+) $ / ) {
        # In the case it's a compound format that is supposed to be merged, then
        # check extensions of both video and audio. If they are '.mp4' and
        # '.m4u', the resulting file extension would be '.mp4', if both of them
        # are '.webm', then the resulting extension it to be the same, and if
        # some other combination, then the resulting file would most likely have
        # '.mkv' extension.
        my ( $v, $a ) = |$/.map: { ext-by-format-id( .Str ) };
        if ( $v eq 'mp4' && $a eq 'm4a' ) {
          $ext = 'mp4';
        }
        elsif ( $v eq 'webm' && $a eq 'webm' ) {
          $ext = 'webm';
        }
        else {
          $ext = 'mkv';
        }
      }
      else {
        $ext = ext-by-format-id( $json<format_id> );
      }
      if ( $ext ) {
        %ret<file-name> = $json-file-name;
        %ret<file-name> ~~ s:i/\.info\.json\s*$/.$ext/;
      }
      else {
        %ret<file-name> = '';
      }
      %ret<size-bytes> = 0;
    }
    if ( $json-file-name && $json-file-name.IO.f ) {
      $json-file-name.IO.unlink;
    }
    return %ret;
  }
}

# NB: Private class, only supposed to be used in Dispatcher for per-URL failure
# counts
class StoredStrIntMap {

   has $.storage-file;

   has %map;

   method read() {
     %map = Empty;
     return if ! $.storage-file.IO.f;
     for $.storage-file.IO.lines -> $l {
       next if $l ~~ m/ ^ \s* '#' /;
       if ( $l ~~ m/ ^ (.+?) \t (\d+) $ / ) {
         %map{ $/[0].Str } = $/[1].Int;
       }
     }
   }

   method write() {
     my $out = '';
     for %map.kv -> $k, $v {
       $out ~= "{$k}\t{$v}\n";
     }
     spurt $.storage-file, $out;
   }

   method increment( Str $k ) {
     self.read();
     %map{ $k } += 1;
     self.write();
     return %map{ $k };
   }

   method reset( Str $k ) {
     self.read();
     %map{ $k }:delete;
     self.write();
   }
}

class TagManager {

  # The structure holding the details of how each tag affects execution. Tags
  # themselves are the first level of hash keys. The detail type is second
  # level keys, the list of supported: 'downloader', 'downloader-switches',
  # 'subdir', 'url-converter' (subref), 'url-to-file-name' (subref),
  # 'url-regexps'
  has %.tag-descriptors = Empty;

  # The tags that should be applied if no tags are specified for a download in
  # incoming/work-queue file
  has @.default-tags = Empty;

  # Tags attached to the current download (as read from 'downloading' control
  # file)
  has @!tags;

  # TODO: Make tags case-insensitive
  multi method set-tags( @new-tags ) {
    @!tags = @new-tags.map: { .trim };
  }

  multi method set-tags( Str $url, @new-tags ) {
    self.set-tags( @new-tags );

    for %.tag-descriptors.keys -> $tag {
      if %.tag-descriptors{$tag}<url-regexps> {
        for |%.tag-descriptors{$tag}<url-regexps> -> $re {
          if $url ~~ $re && $tag ne any( @!tags ) {
            @!tags.append: $tag;
          }
        }
      }
    }
  }

  method get-tags() {
    return @!tags;
  }

  method get-active-tag-list() {
    return @!tags if @!tags;
    return @.default-tags;
  }

  method get-downloader-switches() {
    my @dlsw = Empty;
    for self.get-active-tag-list() -> $tag {
      if ( %.tag-descriptors{$tag}<downloader-switches>:exists ) {
        my @clsw = %.tag-descriptors{$tag}<downloader-switches>;
        @dlsw.append: @clsw;
      }
    }
    return @dlsw;
  }

  # TODO: This just returns subdir from the first tag that has it. It may be
  # re-made to support a multilevel directory tree
  method get-subdir() {
    for self.get-active-tag-list() -> $tag {
      if %.tag-descriptors{$tag}<subdir>:exists {
        my $subdir = %.tag-descriptors{$tag}<subdir>.trim;
        $subdir ~~ s/ \/ $ //;
        return $subdir;
      }
    }
  }

  method get-url-converter() returns Code {
    for self.get-active-tag-list() -> $tag {
      if %.tag-descriptors{$tag}<url-converter>.^name eq 'Sub' {
        return %.tag-descriptors{$tag}<url-converter>;
      }
    }
    return;
  }

  method get-url-to-file-name() returns Code {
    for self.get-active-tag-list() -> $tag {
      if %.tag-descriptors{$tag}<url-to-file-name>.^name eq 'Sub' {
        return %.tag-descriptors{$tag}<url-to-file-name>;
      }
    }
    return;
  }

  method get-downloader() returns Download {
    for self.get-active-tag-list() -> $tag {
      if %.tag-descriptors{$tag}<downloader> ~~ Download {
        return %.tag-descriptors{$tag}<downloader>.clone;
      }
    }
    return;
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
  has Download $!initial-downloader;
  method downloader is rw { $.d }

  has TagManager $!tm = TagManager.new;
  method tag-descriptors is rw { $!tm.tag-descriptors }
  method default-tags is rw { $!tm.default-tags; }

  submethod TWEAK ( Download :$downloader, :%tag-descriptors, :@default-tags ) {
    # A longer alias for $.d allows to clarify meaning in constructor but still
    # use shorter attribute name inside the class
    self.d //= $downloader;

    $!tm.tag-descriptors = %tag-descriptors if %tag-descriptors;
    $!tm.default-tags = @default-tags if @default-tags;
  }

  # Number of sequential failures before download gets rescheduled
  has Int $.failures-before-reschedule = 3;

  # Number of sequential failures before rescheduled download gets abandoned
  has Int $.failures-before-stop = 6; # i.e. two after reschedule

  has StoredStrIntMap $failure-counter = StoredStrIntMap.new(
    :storage-file( self.control-file( 'restart-counts' ) )
  );

  # Redefining this subroutine allows to make dispatcher stop downloads at
  # certain time of day or depending on external process presence.
  # NB: It depends on 'kill' system utility.
  has Code $.download-allowed is rw = sub { return True; }

  # If this is set to True then don't wait until next crontab cycle to schedule
  # next download and start it from schedule immediately as well.
  has Bool $.take-next-download-wo-delay = True;

  has Code $.download-fault-notifier is rw = sub ( Str $msg ) {
    say( "DOWNLOAD FAULT NOTIFICATION: {$msg}" );
  }

  method main() {
    self.copy-from-incoming();
    # The maximum number of actions that can be executed within one crontab
    # invocation is three:
    # 1. Finish previous download in check-restart-download()
    # 2. schedule-next-download()
    # 3. Start the scheduled download using check-restart-download() again
    # Each of them takes a separate crontab invocation unless
    # $.take-next-download-wo-delay is set (thus causing a time gap between
    # subsequent downloads).
    loop ( my $i = 0; $i < ( $.take-next-download-wo-delay ?? 3 !! 1 ); $i++ ) {
      my @cur = self.get-current-download();
      if ( @cur ) {
        $!tm.set-tags( @cur[0], ( @cur[2] || '' ).split(',') );
        self.assign-downloader();
        last unless self.check-restart-download( @cur[0], @cur[1].Int );
      }
      else {
        last unless self.schedule-next-download();
      }
    }
  }

  method schedule-next-download() {
    while ( my $url = self.next-download() ) {
      return True if ( self.schedule-download( $url ) );
    }
    return False;
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
    spurt $fn, '' if $cont;
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

  method schedule-download( Str $url-ext ) returns Bool {
    my ( $url0, $tags ) = $url-ext.split("\t");
    if ( $tags ) {
      $!tm.set-tags( $url0, $tags.split(',') );
    }
    else {
      $!tm.set-tags( $url0, Empty );
    }
    self.assign-downloader();
    $.d.reset-additional-command-line-switches();
    self.pass-tag-cl-switches-to-downloader();

    my $url = self.convert-url( $url0 );

    my %file-params = $.d.get-file-params-cached( $url );
    my $target-size = %file-params<size-bytes> || 0;

    my $fn0 = self.control-file( 'work-queue' );
    my $fn = self.control-file( 'downloading' );

    if ( ! $fn0.IO.e ) {
      self.post-error-message( "Can't find file {$fn0}" );
      return False;
    }

    if ( $fn.IO.e && self.get-current-download() ) {
      self.post-error-message( "{$fn} is not empty" );
      return False;
    }

    # TODO: Maybe need a more robust solution for removing lines from source
    my @lines = $fn0.IO.lines.grep( { ! /$url0/ && ! / ^ \s* $ / } );
    spurt $fn0, @lines.join("\n") ~ ( @lines ?? "\n" !! '' );

    if ( ! %file-params<file-name> && ! self.url-to-file-name( $url0 ) ) {
      say "Can't derive file name for {$url0} - skipping";
      return False;
    }

    spurt $fn, "{$url0}\t{$target-size}"
             ~ ( $tags ?? "\t{$tags}" !! '' ) ~ "\n";

    self.post-log-message( "Scheduled {$url0} for download" );

    return True;
  }

  method get-current-download() {
    my $fn = self.control-file( 'downloading' );
    if ( $fn.IO.e ) {
      for $fn.IO.lines -> $line {
        my @cols = $line.split( "\t" );
        if ( @cols.elems > 1 ) {
          # it's supposed to be formatted like: URL, size (bytes), tags (optional)
          return @cols;
        }
      }
    }
    return Empty;
  }

  # NB: Returns True if it removed current download because it ended or shows
  # failures.
  method check-restart-download( Str $url0, Int $size ) returns Bool {

    self.pass-tag-cl-switches-to-downloader();

    my $url = self.convert-url( $url0 );

    if ( ! $.d.download-process-exists( $url ) ) {
      my %download-status = self.check-download( $url0, $url, $size );
      if %download-status<finished>:exists {
        return %download-status<finished>;
      }
      my $start-needed = %download-status<start-needed>;
      # NB: One of enum vals evaluates to 0 so need to check for definedness
      if ( ( $start-needed // -1 ) !== -1 ) {
        if ( self.restart-download(
          $url0, $url, %download-status<file-name>, $start-needed
        ) ) {
          return True;
        }
      }
    }
    else {
      if ( $.download-allowed.() ) {
        self.post-log-message( "URL download underway: {$url0}" );
        $failure-counter.reset( $url0 );
      }
      else {
        self.post-log-message( "Stopping download due to schedule restriction "
                             ~ "in download-allowed(): {$url0}" );
        $.d.stop-download( $url );
      }
    }
    return False;
  }

  method check-download( Str $url0, Str $url, Int $size ) {
    enum StartType < start restart >;

    my %ret = Empty;

    if ( ! $.download-allowed.() ) {
      self.post-log-message( "Not starting download of {$url0} because of "
                           ~ "schedule restriction in download-allowed()" );
      %ret<finished> = False;
      return %ret;
    }

    # NB: It's important to have consistent file name over time, so either
    # get it from downloader or calculate from URL.
    my $file-name = $.d.get-file-params-cached( $url )<file-name>
                 // self.url-to-file-name( $url0 );

    unless ( %ret<file-name> = $file-name ) {
      self.post-log-message( "Can't derive target file name for {$url0}" );
    }

    if ( my $subdir = $!tm.get-subdir ) {
      $file-name = $subdir ~ '/' ~ $file-name;
    }

    my $target_fn = $.d.download-dir ~ '/' ~ $file-name;
    if ( $file-name && $target_fn.IO.e && $target_fn.IO.f ) {
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
        self.finish-download( $url0 );
        $failure-counter.reset( $url0 );
        %ret<finished> = True;
        return %ret;
      }
      else {
        %ret<start-needed> = restart;
      }
    }
    else {
      %ret<start-needed> = start;
    }
    return %ret;
  }

  method restart-download( Str $url0, Str $url, $file-name, $start-needed ) {
    given self.register-restart( $url0 ) {
      when .Str eq 'restart' {
        # Additional protection against calling $.d.start-download() without
        # posting a message in logs.
        my Bool $start-flag = False;
        given $start-needed {
          when .Str eq 'start' {
            self.post-log-message( "Started new URL download: {$url0}" );
            $start-flag = True;
          }
          when .Str eq 'restart' {
            self.post-log-message( "Restarted incomplete URL download: "
                                 ~ $url0 );
            $start-flag = True;
          }
        }
        if ( $start-flag ) {
          my $fn = $file-name;
          if ( my $subdir = $!tm.get-subdir ) {
            $fn = $subdir ~ '/' ~ $file-name;
            $.d.check-create-subdir( $subdir );
          }
          $.d.start-download( $url, $fn );
        }
        return False;
      }
      when .Str eq 'delay' {
        self.post-log-message( "Placing faulty download at the end of work "
                             ~ "queue: {$url0}" );
        self.finish-download( $url0, :delay(True) );
        return True;
      }
      when .Str eq 'abandon' {
        self.post-log-message( "Download eventually failed: {$url0}" );
        $.download-fault-notifier.( "Download failed: {$url0}" );
        self.finish-download( $url0, :failed(True) );
        $failure-counter.reset( $url0 );
        return True;
      }
    }
  }

  method finish-download( Str $url, Bool :$failed, Bool :$delay ) {

    if ( $failed && $delay ) {
      self.post-error-message(
        Q<Can't use finish-download() with both $failed and $delay params>
      );
    }

    my $fn0 = self.control-file( 'downloading' );
    my $fn = self.control-file( 'complete' );
    if ( $failed ) {
      $fn = self.control-file( 'failed' );
    }
    if ( $delay ) {
      $fn = self.control-file( 'work-queue' );
    }

    if ( ! $fn0.IO.e ) {
      self.post-error-message( "Can't find file {$fn0}" );
      return;
    }

    # NB: 'downloading' file only supposed to have only one line
    my $l = $fn0.IO.lines[0];
    if ( $l ~~ m:i/ $url / ) {
      # Don't copy file size into work queue, but copy previously saved tags
      if ( $delay ) {
        my @tgs = $!tm.get-tags();
        $l = $url ~ ( @tgs ?? "\t" ~ @tgs.join(',') !! '' );
      }
      spurt $fn, $l ~ "\n", :append;
      spurt $fn0, '';
    }
    else {
      self.post-error-message( "First line of {$fn0} doesn't contain required "
                             ~ "URL {$url}" );
    }
  }

  method register-restart( Str $url ) {
    my enum Action < restart delay abandon >;
    my $ret = restart;
    my $c = $failure-counter.increment( $url );
    if ( $c == $.failures-before-reschedule + 1 ) {
      $ret = delay;
    }
    if ( $c > $.failures-before-stop ) {
      $ret = abandon;
    }
    return $ret;
  }

  method convert-url( Str $url0 ) {
    if my $from-tags = $!tm.get-url-converter() {
      return $from-tags( $url0 );
    }
    return %.url-converters<url>( $url0 );
  }

  method url-to-file-name( Str $url0 ) {
    if my $from-tags = $!tm.get-url-to-file-name() {
      return $from-tags( $url0 );
    }
    return %.url-converters<file-name>( $url0 );
  }

  method assign-downloader() {
    unless ( $!initial-downloader ) {
      $!initial-downloader = $.d;
    }
    if ( my $d = $!tm.get-downloader() ) {
      $d.download-dir ||= $!initial-downloader.download-dir;
      if (
          $d.^lookup('limit-rate')
          &&
          $!initial-downloader.^lookup('limit-rate')
      ) {
        $d.limit-rate ||= $!initial-downloader.limit-rate;
      }
      $.d = $d;
    }
    else {
      $.d = $!initial-downloader;
    }
  }

  method pass-tag-cl-switches-to-downloader() {
    $.d.merge-in-command-line-switches( $!tm.get-downloader-switches );
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
