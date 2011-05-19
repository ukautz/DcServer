package DcServer;

use lib '.';
use threads;
use threads::shared;
use Thread::Queue;
use IO::Socket;
use IO::Async::Loop;
use IO::Async::Listener;
use Time::HiRes qw/sleep/;
use strict;
use warnings;

my $stop :shared;
my $accept_queue= Thread::Queue->new;
my $closed_queue= Thread::Queue->new;

sub new {
  # Params: host, port, thread_count, eom_marker, main_yield, main_cb,
  # done_cb, processor_cb
  my ($proto, %param)= @_;
  my $class= ref($proto) || $proto;
  bless +{
    socket_defaults => +{
      LocalHost => $param{host} || 'localhost',
      LocalPort => $param{port} || 8191},
    thread_count => $param{thread_count} || 10,
    main_yield => $param{main_yield} || 5,
    main_cb => $param{main_cb} || sub {},
    done_cb => $param{done_cb} || sub {},
    processor_cb => $param{processor_cb} || \&processor,
    eom_marker => $param{eom_marker} || "\\n\\.\\n",
    thread_pool => undef
  } => $class;
}

# This callback (for processor_cb) does sommething stupid with the string
# that the client sends to the server, then returns the new string. This
# code hopefully illustrates how to put together a callback function for
# processing data from clients.
sub processor {
  my ($data, $ip, $tid, $fnstop)= @_;
  "[tid=$tid; ip=$ip] " . join('', reverse(split //, $data));
}

sub start {
  my $self= shift;
  
  # do not die in SIGPIPE
  local $SIG{PIPE} = 'IGNORE';

  # Start a thread to dispatch incoming requests
  threads->create(sub {$self->accept_requests})->detach;

  # Start the thread pool to handle dispatched requests
  for (1 .. $self->{thread_count}) {
    threads->create(sub {$self->request_handler})->detach}

  # Start a loop for performing tasks in the background, while
  # handling requests
  $self->main_loop;

  $self->{done_cb}->();
}

sub stop {
  my $self= shift;
  $stop= 1;
}

sub main_loop {
  my $self= shift;
  my $counter= 1;
  until($stop) {
    $self->{main_cb}->($counter++, sub {$self->stop});
    sleep $self->{main_yield};
  }
}

sub accept_requests {
  my $self= shift;
  my ($csocket, $n, %socket);
  
  my $loop = IO::Async::Loop->new;
  my $listener = IO::Async::Listener->new(
    on_stream => sub {
      ( undef, my $stream ) = @_;
      
      $stream->configure(
        on_read => sub {
          my ( $handle, $buffer_ref, $is_eof ) = @_;
          return if $is_eof;
          $n = $handle->write_fileno;
          $socket{ $n } = $handle;
          $$buffer_ref =~ s/$self->{eom_marker}\z//m;
          $accept_queue->enqueue( [
            $n, inet_ntoa( $handle->read_handle->peeraddr ), $$buffer_ref ] );
          $$buffer_ref = '';
          return 0;
        }
      );
      $loop->add( $stream );
    }
  );
  $loop->add( $listener );
  
  
  my $lsocket= new IO::Socket::INET(
    %{$self->{socket_defaults}},
    Proto => 'tcp',
    Listen => 1,
    Reuse => 1);
  die "Can't create listerner socket. Server can't start. $!." unless $lsocket;
  
  $listener->listen( handle => $lsocket );
  
  until ( $stop ) {
    $loop->loop_once(0.01);
    while( $n = $closed_queue->dequeue_nb ) {
      if ( defined( my $sock = delete $socket{ $n } ) ) {
        my $wh = $sock->write_handle;
        $wh->shutdown(2) if defined $wh;
      }
    }
  }
}

sub request_handler {
  my $self= shift;
  my ($n, $ip, $data);
  my ($receive_time, $process_time, $send_time);
  until($stop) {
    ($n, $ip, $data )= @{ $accept_queue->dequeue };
    next unless $n;
    my $write_sock = IO::Socket::INET->new_from_fd( $n, '>' );
    die "Could not open write socket: $@" unless $write_sock;
    $write_sock->write( $self->{processor_cb}->(
      $data, $ip, threads->tid, sub {$self->stop}
    ). "\n.\n" );
    #$write_sock->flush;
    $write_sock->close;
    $closed_queue->enqueue($n)}
}


1;
