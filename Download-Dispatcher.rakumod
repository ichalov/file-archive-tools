=begin pod

=head1 Download-Dispatcher.rakumod

Classes that help to create HTTP download queue crontab scripts.

An example of crontab script that could be used with this module:

=begin code
use lib <.>;
use Download-Dispatcher;

my $dl = Wget-Download.new: :limit-rate(102400) :download-dir('.');
my $d = Dispatcher-MySQL.new:
  :downloader($dl) :dispatcher-dir('.')
  :db-name('download-dispatcher') :db-user('dd');

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
  'vimeo' => %(
    'url-regexps' => ( rx:i/ https? '://vimeo.' /, ),
    'downloader' => Vimeo-Download.new( :limit-rate(51200) ),
    'subdir' => 'video',
  ),
);

$d.main();
=end code

=head2 TODO

=item Implement priority levels

=head2 AUTHOR

Victor Ichalov <ichalov@gmail.com>, 2020

=end pod

unit module Download-Dispatcher;

# NB: The main class in this module is Dispatcher. It's located towards bottom
# of the file because of the dependencies that have to enter earlier.

# NB: Most of the control files are only used in generic Dispatcher. DB-based
# dispatchers are only supposed to use incoming and md5sums (and hold all other
# state info in a database table).
my %dispatcher-storage = (
  incoming => 'incoming.txt',
  work-queue => 'download.txt',
  downloading => 'downloading.txt',
  restart-counts => 'restart-counts.txt',
  failed => 'failed.txt',
  complete => 'complete.txt',
  md5sums => 'md5sums.txt',
);

role Download is export {

  has $.download-dir is rw = '.';

  has %.sysutils =
    'ps' => '/bin/ps',
    'screen' => '/usr/bin/screen',
    'kill' => '/bin/kill',
  ;
  has Bool $.autostart-called is rw = False;

  has @.additional-command-line-switches = Empty;
  has @!initial-command-line-switches = Empty;

  submethod TWEAK ( :@additional-command-line-switches ) {
    @!initial-command-line-switches = @additional-command-line-switches;
  }

  has %.file-params-cache = Empty;

  method sysutils-dependency-check() {
    my @missing;
    for %.sysutils.kv -> $cmd, $file {
      unless $file.IO.e {
        @missing.append: $cmd;
      }
    }
    if ( @missing ) {
      die "Can't find required system utilities: "
        ~ @missing.map({%.sysutils{$_}}).join(', ');
    }
  }

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
      run( %.sysutils<kill>, '-9', $pid );
    }
  }

  method get-file-params-cached( Str $url ) returns Hash {

    # HACK: This is supposed to be the first call after initialization to any
    # Download class so perform autostart here
    unless ( $.autostart-called ) {
      self.AUTOSTART();
      $.autostart-called = True;
    }

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

  # TODO: Make downloaders save into subdir without chdir()
  method chdir-into-target( $file-name ) {
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
  }

}

class Wget-Download does Download is export {

  has $.limit-rate is rw; # in bytes per second

  has $!wget = '/usr/bin/wget';

  method AUTOSTART() {
    %.sysutils<wget> = $!wget;
    self.sysutils-dependency-check();
  }

  method start-download( $url, $file-name ) {
    my @cmd = ( %.sysutils<screen>, '-d', '-m', $!wget, '-c' );
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
    my $proc = run( %.sysutils<ps>, 'auxww', :out );
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

# NB: Use sftp and not scp because it has download continuation (-a option)
# TODO: Research if rsync can do better
class SFTP-Download does Download is export {

  has $.limit-rate is rw; # in bytes per second

  has $!sftp = '/usr/bin/sftp';

  method AUTOSTART() {
    %.sysutils<sftp> = $!sftp;
    self.sysutils-dependency-check();
  }

  method host-file-from-url( Str $url ) {
    # TODO: Make a more reliable regexp
    if ( $url ~~ m:i/ ^ (\w+) '://' (.+?) ':' (.+) / ) {
      if ( ! $/[0].lc eq any( 'scp', 'sftp' ) ) {
        die "SFTP-Download can only process scp:// or sftp://";
      }
      return %( 'host' => $/[1].Str, 'file' => $/[2].Str );
    }
  }

  method start-download( $url, $file-name ) {

    self.chdir-into-target( $file-name );

    my @cmd = ( %.sysutils<screen>, '-d', '-m', $!sftp, '-a' );
    if ( $.limit-rate ) {
      my $kbits-per-sec = ( $.limit-rate / 128 ).round();
      @cmd.push: '-l', $kbits-per-sec;
    }
    if ( @.additional-command-line-switches ) {
      @cmd.append: @.additional-command-line-switches;
    }
    my %hf = self.host-file-from-url( $url );
    @cmd.push: %hf<host> ~ ':' ~ %hf<file>;
    run( @cmd );
  }

  method download-process-exists( Str $url ) returns Int {
    my $proc = run( %.sysutils<ps>, 'auxww', :out );
    my $out = $proc.out.slurp;

    my %hf = self.host-file-from-url( $url );
    my $url-part = %hf<host> ~ ':' ~ %hf<file>;

    if ( $out ~~ self.process-exists-regex( 'sftp', $url-part ) ) {
      return $/[0].Int;
    }
    return 0;
  }

  method get-file-params( Str $url ) returns Hash {

    my %hf = self.host-file-from-url( $url );
    my $proc = run(
      $!sftp, |@.additional-command-line-switches, %hf<host>, :out, :err, :in
    );
    $proc.in.print: "ls -l {%hf<file>}\nexit\n";
    my $out = $proc.out.slurp;

    # TODO: Make a more reliable regexp
    if ( $out ~~ m/ ^^ [\S+\s+]**4 (\d+) .+? {%hf<file>} \s* $$ / ) {
      my %ret;
      %ret<size-bytes> = $/[0].Int;
      %ret<file-name> = %hf<file>;
      %ret<file-name> ~~ s:i/ .+ '/' //;
      return %ret;
    }
    else {
      return %();
    }
  }
}

class YT-DL-Download does Download is export {

  has $.limit-rate is rw; # in bytes per second

  has $.youtube-dl = '/usr/bin/youtube-dl';

  method AUTOSTART() {
    %.sysutils<youtube-dl> = $.youtube-dl;
    self.sysutils-dependency-check();
  }

  method start-download( $url, $file-name ) {

    self.chdir-into-target( $file-name );

    my @cmd = ( %.sysutils<screen>, '-d', '-m', $.youtube-dl );
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
    my $proc = run( %.sysutils<ps>, 'auxww', :out );
    my $out = $proc.out.slurp;

    if ( $out ~~ self.process-exists-regex( 'youtube-dl', $url ) ) {
      return $/[0].Int;
    }
    return 0;
  }

  method get-file-params( Str $url ) returns Hash {

    my %ret = Empty;

    self.chdir-into-target( '' );

    my $proc = run(
      $.youtube-dl, |@.additional-command-line-switches,
      '--write-info-json', '--skip-download', $url, :out, :err
    );
    my $out = $proc.out.slurp;
    my $json-file-name;
    if ( $out ~~ m:i/
      ^^ '[info] Writing video description metadata as JSON to: ' \s* (.+) $$
    / ) {
      $json-file-name = $/[0].Str;

      if ( $json-file-name && $json-file-name.IO.f ) {
        %ret = self.parse-json( $json-file-name.IO.slurp, $json-file-name );
        $json-file-name.IO.unlink;
      }
    }
    return %ret;
  }

  method parse-json( Str $json-text, Str $json-file-name ) {

    use JSON::Tiny;

    my %ret = Empty;

    if ( my $json = from-json( $json-text ) ) {
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
    return %ret;
  }
}

class Vimeo-Download is YT-DL-Download is export {

  method parse-json( Str $json-text, Str $json-file-name ) {

    use JSON::Tiny;

    my %ret = Empty;

    if ( my $json = from-json( $json-text ) ) {
      %ret<file-name> = $json<_filename>;
      %ret<size-bytes> = 0;
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

  multi method set-tags( @new-tags ) {
    @!tags = @new-tags.map: { .trim.lc };
  }

  multi method set-tags( Str $url, @new-tags ) {
    self.set-tags( @new-tags );

    for %.tag-descriptors.keys -> $tag {
      if %.tag-descriptors{$tag}<url-regexps> {
        for |%.tag-descriptors{$tag}<url-regexps> -> $re {
          if $url ~~ $re && $tag ne any( @!tags ) {
            @!tags.append: $tag.lc;
          }
        }
      }
    }
  }

  method get-tags() {
    return @!tags.grep: { $_ };
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
  }

  method get-url-to-file-name() returns Code {
    for self.get-active-tag-list() -> $tag {
      if %.tag-descriptors{$tag}<url-to-file-name>.^name eq 'Sub' {
        return %.tag-descriptors{$tag}<url-to-file-name>;
      }
    }
  }

  method get-downloader() returns Download {
    for self.get-active-tag-list() -> $tag {
      if %.tag-descriptors{$tag}<downloader> ~~ Download {
        return %.tag-descriptors{$tag}<downloader>.clone;
      }
    }
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

  has StoredStrIntMap $.failure-counter is rw = StoredStrIntMap.new(
    :storage-file( self.control-file( 'restart-counts' ) )
  );

  # Redefining this subroutine allows to make dispatcher stop downloads at
  # certain time of day or depending on external process presence.
  has Code $.download-allowed is rw = sub { return True; }

  # If this is set to True then don't wait until next crontab cycle to schedule
  # next download and start it from schedule immediately as well.
  has Bool $.take-next-download-wo-delay = True;

  has Str $.priority-tag = 'priority';

  has Code $.download-fault-notifier is rw = sub ( Str $msg ) {
    say( "DOWNLOAD FAULT NOTIFICATION: {$msg}" );
  }

  # await() on this in order to get md5sum of just downloaded file saved
  has $!md5sum-done;

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
        repeat {
          # NB: Need to reset tags in case of switch to priority
          $!tm.set-tags( Empty );
          $!tm.set-tags( @cur[0], ( @cur[2] || '' ).split(',') );
          self.assign-downloader();
        } while self.check-switch-priority( @cur ); # NB: this may change @cur
        last unless self.check-restart-download( @cur[0], @cur[1].Int );
      }
      else {
        last unless self.schedule-next-download();
      }
    }

    if ( $!md5sum-done ~~ Promise ) {
      await( $!md5sum-done );
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

  method check-switch-priority( @cur ) {
    my @tags = $!tm.get-active-tag-list;
    unless ( $.priority-tag eq any( @tags ) ) {
      if ( my @priority = self.next-download( :priority-only(True) ) ) {
        my $cur_url = @cur[0];
        self.post-log-message( "Switch current {$cur_url} download to "
                             ~ @priority[0] ~ '.' );
        $.d.stop-download( self.convert-url( $cur_url ) );
        self.finish-download( $cur_url, :push(True) );
        self.schedule-download( @priority.join("\t") );
        @cur = self.get-current-download();
        return True;
      }
    }
    return False;
  }

  method next-download( Bool :$priority-only ) {
    my $fn = self.control-file( 'work-queue' );
    if ( $fn.IO.e ) {
      my @normal;
      my @priority;
      for $fn.IO.lines -> $url {
        if ( $url && $url !~~ m/^\s*\#/ ) {
          my $priority-tag = $.priority-tag;
          if ( $url ~~ m/^\T+\t\T*<wb> $priority-tag <wb>/ ) {
            @priority.append: $url;
          }
          else {
            @normal.append: $url;
          }
        }
      }
      if ( @priority.elems ) {
        return @priority[0];
      }
      elsif( @normal.elems && ! $priority-only ) {
        return @normal[0];
      }
    }
    self.post-log-message( "Nothing to do" ) unless $priority-only;
    return Empty;
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

    if ( ! %file-params<file-name> && ! self.url-to-file-name( $url0 ) ) {
      say "Can't derive file name for {$url0} - skipping";
      return False;
    }

    self.move-from-queue-to-scheduled( $url0, $target-size );

    self.post-log-message( "Scheduled {$url0} for download" );

    return True;
  }

  method move-from-queue-to-scheduled( Str $url0, Int $target-size ) {

    my $fn = self.control-file( 'downloading' );
    if ( $fn.IO.e && self.get-current-download() ) {
      die "{$fn} is not empty inside schedule-download()";
    }

    spurt $fn, "{$url0}\t{$target-size}"
             ~ ( $!tm.get-tags() ?? "\t" ~ $!tm.get-tags().join(',') !! '' ) ~ "\n";

    my $fn0 = self.control-file( 'work-queue' );

    if ( ! $fn0.IO.e ) {
      die "Can't find file {$fn0} inside schedule-download()";
    }

    # TODO: Maybe need a more robust solution for removing lines from source
    my @lines = $fn0.IO.lines.grep( { ! /$url0/ && ! / ^ \s* $ / } );
    spurt $fn0, @lines.join("\n") ~ ( @lines ?? "\n" !! '' );
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
        $.failure-counter.reset( $url0 );
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

    my $target-fn = $.d.download-dir ~ '/' ~ $file-name;
    if ( $file-name && $target-fn.IO.e && $target-fn.IO.f ) {
      my $d_size = $target-fn.IO.s;
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
        $.failure-counter.reset( $url0 );

        # Run a parallel process to register the just downloaded file's md5sum
        my $md5calc = Proc::Async.new( '/usr/bin/md5sum', $target-fn );
        $md5calc.stdout.tap( -> $buf {
          spurt self.control-file( 'md5sums' ), $buf, :append
        } );
        $!md5sum-done = $md5calc.start;

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
        $.failure-counter.reset( $url0 );
        return True;
      }
    }
  }

  method finish-download( Str $url, Bool :$failed, Bool :$delay, Bool :$push ) {

    if ( ( $failed, $delay, $push ).grep({$_}).elems > 1 ) {
      self.post-error-message(
        Q<Can't use finish-download() with more than one of $failed, $delay >
      ~ Q<or $push params set>
      );
    }

    my $fn0 = self.control-file( 'downloading' );
    my $fn = self.control-file( 'complete' );
    if ( $failed ) {
      $fn = self.control-file( 'failed' );
    }
    if ( $delay || $push ) {
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
      if ( $delay || $push ) {
        my @tgs = $!tm.get-tags();
        $l = $url ~ ( @tgs ?? "\t" ~ @tgs.join(',') !! '' );
      }
      if ( ! $push ) {
        spurt $fn, $l ~ "\n", :append;
      }
      else {
        my $cont = $fn.IO.slurp;
        spurt $fn, $l ~ "\n" ~ $cont;
      }
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
    my $c = $.failure-counter.increment( $url );
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
      if ( ! $d.download-dir || $d.download-dir eq '.' ) {
        my $dd = $!initial-downloader.download-dir;
        if ( $dd && $dd ne '.' ) {
          $d.download-dir = $dd;
        }
      }
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

  # NB: tm property can't be made public because it participates in a submethod.
  # This getter is added so that $!tm could be accessed in inherited classes.
  method get-tm() {
    return $!tm;
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

class StoredStrIntMap-MySQL is StoredStrIntMap {
  has $.dbh;
  has $.table-name;

  method set( Str $url, Str $count-expr ) {
    my $update-sql = Q:c:to/SQL/;
    update {$.table-name}
    set current_failures = {$count-expr}
    where url=?
    SQL

    my $sth = $.dbh.db.prepare( $update-sql );
    $sth.execute( $url );
  }

  method reset( Str $url ) {
    self.set( $url, '0' );
  }

  method increment( Str $url ) {
    self.set( $url, 'current_failures + 1' );

    my @rows = |$.dbh.query(
      "select current_failures from {$.table-name} where url = ?", $url
    ).hashes;
    return @rows[0]<current_failures> if @rows;
  }
}

class Dispatcher-MySQL is Dispatcher is export {

  has $!dbh;
  has $.db-host is rw = '';
  has $.db-port is rw = 3306;
  has $.db-name is rw = '';
  has $.db-user is rw = '';
  has $.db-password is rw = '';

  has $!sth-add-to-queue;

  has $.queue-tbl is rw = 'download_queue';

  method main() {
    self.init-db();
    nextsame;
  }

  # TODO: Research how not to call from main()
  method init-db() {
    try require DB::MySQL;
    if ( ! $! ) {
      $!dbh = ::('DB::MySQL').new:
        :host($.db-host) :port($.db-port) :database($.db-name)
        :user($.db-user) :password($.db-password)
      ;
      self.check-create-tables();

      $!sth-add-to-queue = $!dbh.db.prepare( Q:c:to/SQL/ );
      insert into {$.queue-tbl} (url, tags) values (?, ?)
      SQL

      $.failure-counter = StoredStrIntMap-MySQL.new(
        :dbh($!dbh) :table-name($.queue-tbl)
      );
    }
    else {
      die "Using Dispatcher-MySQL requires DB::MySQL module";
    }
  }

  method check-create-tables() {
    $!dbh.execute( Q:c:to/SQL/ );
    create table if not exists {$.queue-tbl} (
      id int not null primary key auto_increment,
      url varchar(768) not null,
      tags varchar(768),
      file_size int default 0,
      created datetime default now(),
      first_start datetime,
      started datetime,
      pushed datetime,
      complete datetime,
      failed datetime,
      current_failures int default 0,
      unique key (url)
    )
    SQL

    # Migration after implementing priority tag
    unless
      $!dbh.query( "show columns from {$.queue-tbl} like 'pushed'" )
        .arrays.elems
    {
      $!dbh.execute( Q:c:to/SQL/ );
      alter table {$.queue-tbl}
      add pushed datetime after started
      SQL
    }
  }

  method copy-from-incoming() {
    my $fn = self.control-file( 'incoming' );
    my Bool $found-records = False;
    if ( $fn.IO.e ) {
      for $fn.IO.lines -> $l {
        $found-records = True;
        my ( $url, $tags ) = |$l.split("\t");
        my $added = self.add-download( $url, $tags );
        self.post-log-message( "Queued incoming download: {$url}" ) if $added;
      }
    }
    spurt $fn, '' if $found-records;
  }

  method add-download( Str $url, $tags? ) {
    my Bool $added = True;
    try {
      $!sth-add-to-queue.execute( $url, $tags );
      CATCH {
        when .Str ~~ m:i/^ 'duplicate entry' .+? 'for key \'url\''/ {
          self.post-log-message( "URL {$url} already exists in the queue" );
          $added = False;
        }
      }
    }
    return $added;
  }

  method next-download( Bool :$priority-only ) {

    my @rows;
    my $priority-expr =
      "if(concat(',', tags, ',') like concat('%,', ?, ',%'),1,2)";
    if ( $priority-only ) {
      @rows = |$!dbh.query( Q:c:to/SQL/, $.priority-tag ).hashes;
      select * from {$.queue-tbl}
      where complete is null and failed is null and started is null
      and {$priority-expr} = 1
      order by
        pushed desc, first_start, created, id
      limit 1
      SQL
    }
    else {
      # TODO: In case of $.take-next-download-wo-delay try to use transaction and
      # 'select for update'
      @rows = |$!dbh.query( Q:c:to/SQL/, $.priority-tag ).hashes;
      select * from {$.queue-tbl}
      where complete is null and failed is null
      order by started desc,
        {$priority-expr},
        pushed desc, first_start, created, id
      limit 1
      SQL
    }

    if ( @rows && my %row = @rows[0] ) {
      if ( %row<started> ) {
        die "Found current download inside next-download() call";
      }

      if ( %row<url> ) {
        return %row<url> ~ ( %row<tags> ?? "\t" ~ %row<tags> !! '' );
      }
    }

    self.post-log-message( "Nothing to do" ) unless $priority-only;
    return Empty;
  }

  method move-from-queue-to-scheduled( Str $url0, Int $target-size ) {
    my $update-sql = Q:c:to/SQL/;
    update {$.queue-tbl}
    set first_start = ifnull(first_start, now()),
        started = now(), file_size = ?, tags = ?
    where url = ?
    SQL

    my $sth = $!dbh.db.prepare( $update-sql );
    $sth.execute(
      $target-size, self.get-tm.get-active-tag-list().join(',') || Nil, $url0
    );
  }

  method get-current-download() {
    my @rows = |$!dbh.query(
      "select * from {$.queue-tbl} where not started is null"
    ).hashes;

    if @rows.elems > 1 {
      die "Found more than one current download inside get-current-download()"
        ~ " call";
    }

    if @rows.elems > 0 {
      my %row = @rows[0];
      return ( %row<url>, %row<file_size>, %row<tags> );
    }

    return Empty;
  }

  method finish-download( Str $url, Bool :$failed, Bool :$delay, Bool :$push ) {

    if ( ( $failed, $delay, $push ).grep({$_}).elems > 1 ) {
      self.post-error-message(
        Q<Can't use finish-download() with more than one of $failed, $delay >
      ~ Q<or $push params set>
      );
    }

    my $update-sql = "update {$.queue-tbl} set started = null";
    $update-sql ~= ', failed = Now()' if $failed;
    $update-sql ~= ', pushed = Now()' if $push;
    $update-sql ~= ', complete = Now()' if !$failed && !$delay && !$push;
    $update-sql ~= ' where url=?';

    my $sth = $!dbh.db.prepare( $update-sql );
    $sth.execute( $url );
  }
}
