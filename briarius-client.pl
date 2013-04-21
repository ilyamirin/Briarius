#!/usr/bin/env perl

use Modern::Perl;
use Data::Dumper;
use File::Path qw(make_path remove_tree);
use Log::Log4perl qw(:easy);
use Mojo::UserAgent;
use Mojo::JSON 'j';
use Digest::MD5 qw(md5_hex);

use constant { CHUNK_SIZE => 64000, };

my $args;
my @files           = ();
my $target_to_local = {};
my $chunks          = {};
my $chunks_to_send  = {};
my @files_to_restore = ();

sub get_path_in_target {
    my ( $absolute_path, $outer_path ) = @_;
    if ( $outer_path =~ /^$absolute_path\/(.+)$/ ) {
        return $args->{'--target-id'} . '/' . $1;
    }
    else {
        ERROR "Incorrect path $absolute_path, $outer_path";
    }
}

sub crawl {
    my ($path_to_file) = @_;

    if ( -d $path_to_file ) {
        INFO 'Directory has been found: ' . $path_to_file;

        my $dir;
        opendir $dir, $path_to_file or die $!;

        while ( readdir $dir ) {
            next if $_ =~ /^\.\.?$/;
            $path_to_file .= '/' unless $path_to_file =~ /\/$/;
            crawl( $path_to_file . $_ );
        }

        closedir $dir;

    }
    elsif ( -f $path_to_file ) {
        INFO 'File has been found: ' . $path_to_file;
        my $path_to_file_in_target =
          get_path_in_target( $args->{'-p'}, $path_to_file );
        push @files, $path_to_file_in_target;
        $target_to_local->{$path_to_file_in_target} = $path_to_file;

    }    #elsif

}    #sub process_file

sub slice_file {
    my $path_to_file_in_target = shift;
    my $path_to_file           = $target_to_local->{$path_to_file_in_target};

    open( my $file, '<', $path_to_file ) or die $!;

    $chunks->{$path_to_file_in_target} = [] and return
      if ( stat($file) )[7] == 0;

    INFO $path_to_file;
    flock $file, 1;

    my ( $data, $n );
    while ( ( $n = read $file, $data, CHUNK_SIZE ) > 0 ) {
        push @{ $chunks->{$path_to_file_in_target} }, $data;
    }

    close $file;
}

sub ws_is_next_file_exist {
    my $tx = shift;
    if ( my $file = shift @files ) {
        my $req = {
            request => 'IS_FILE_VERSION_EXISTED',
            client  => $args->{'-n'},
            file    => $file
        };
        $tx->send( j $req );
        return 1;
    }
    else {
        return 0;
    }
}

sub ws_is_next_chunk_existed {
    my ( $tx, $msg ) = @_;
    my $chunk = shift @{ $chunks->{ $msg->{file} } };
    return 0 unless $chunk;
    my $hash = md5_hex $chunk;
    my $req = { request => 'IS_CHUNK_EXISTED', chunk_hash => $hash };
    $req->{client} = $args->{'-n'};
    $req->{$_} = $msg->{$_} for qw/file file_version/;
    $tx->send( j $req );
    $chunks_to_send->{$hash} = $chunk;
}

sub ws_create_file_version {
    my ( $tx, $msg ) = @_;
    my $req = {
        request      => 'CREATE_FILE_VERSION',
        file_version => $msg->{file_version}
    };
    $tx->send( j $req );
}

sub ws_restore_next_file {
    my $tx = shift;
    my $file_version = shift @files_to_restore;
    $tx->finish unless $file_version;
    $tx->send( j { request => 'GET_CHUNK', file_version => $file_version, chunk_number => 1 } );    
}

sub ws_get_next_chunk {
    my ( $tx, $msg ) = @_;
    my $file_version = $msg->{ file_version };
    my $chunk_number = $msg->{ chunk_number };
    $tx->send( j { request => 'GET_CHUNK', file_version => $file_version, chunk_number => ++$chunk_number } );    
}

BEGIN {
    Log::Log4perl->easy_init(
        {
            level  => 'DEBUG',
            file   => "STDOUT",
            layout => '%m%n'
        },
    );

    my $started_at = localtime(time);

    $args = {@ARGV};

    $args->{'-p'} = $1 if $args->{'-p'} =~ /^(.+)\/$/;

    crawl $args->{'-p'};    #crawl all files

    my $ua = Mojo::UserAgent->new;

    $ua->websocket(
        $args->{'-s'} => sub {
            my ( $ua, $tx ) = @_;

            ERROR 'WebSocket handshake failed!' and return
              unless $tx->is_websocket;

            $tx->on(
                finish => sub {
                    my ( $tx, $code, $reason ) = @_;
                    INFO "Session with Briariues-server has been closed.";
                    my $finished_at = localtime(time);
                    INFO "$args->{ '-c' }: $started_at -> $finished_at";
                }
            );

            $tx->on(
                message => sub {
                    my ( $tx, $msg ) = @_;

                    $tx->finish and return unless $msg;

                    #DEBUG "Server response: $msg";

                    $msg = j $msg;
                    if ( $msg->{response} eq 'BACKUP_START_ACCEPTED' ) {
                        INFO 'Backup start accepted by server';
                        ws_is_next_file_exist($tx);
                    }
                    elsif ( $msg->{response} eq 'FILE_VERSION_IS_NOT_EXISTED' )
                    {
                        INFO
                          "File version $msg->{ file_version } is not existed";
                        slice_file $msg->{file};
                        ws_is_next_file_exist($tx)
                          unless ws_is_next_chunk_existed( $tx, $msg );
                    }
                    elsif ( $msg->{response} eq 'FILE_VERSION_IS_EXISTED' ) {
                        INFO "File version $msg->{ file_version } is existed";
                        $chunks->{ $msg->{file} } = undef;
                        $tx->finish unless ws_is_next_file_exist($tx);
                    }
                    elsif ( $msg->{response} eq 'CHUNK_IS_NOT_EXISTED' ) {
                        INFO "Chunk $msg->{ chunk_hash } is not existed.";
                        if ( $chunks_to_send->{ $msg->{chunk_hash} } ) {
                            my $req = {
                                request      => 'LOAD_CHUNK',
                                client       => $args->{'-n'},
                                file         => $msg->{file},
                                file_version => $msg->{file_version},
                                chunk_hash   => $msg->{chunk_hash},
                                chunk => $chunks_to_send->{ $msg->{chunk_hash} }
                            };
                            $tx->send( j $req );
                            $chunks_to_send->{ $msg->{chunk_hash} } = undef;
                        }
                    }
                    elsif ( $msg->{response} eq 'CHUNK_IS_EXISTED' ) {
                        INFO "Chunk $msg->{ chunk_hash } is existed.";
                        my $req = {
                            request      => 'ADD_EXISTED_CHUNK_TO_FILE_VERSION',
                            client       => $args->{'-n'},
                            file         => $msg->{file},
                            file_version => $msg->{file_version},
                            chunk_hash   => $msg->{chunk_hash}
                        };
                        $tx->send( j $req );
                        $chunks_to_send->{ $msg->{chunk_hash} } = undef;
                    }
                    elsif ( $msg->{response} eq 'CHUNK_WAS_LOADED' ) {
                        INFO "Chunk $msg->{ chunk_hash } was loaded.";
                        ws_create_file_version( $tx, $msg )
                          unless ws_is_next_chunk_existed( $tx, $msg );
                    }
                    elsif ( $msg->{response} eq 'FILE_VERSION_WAS_CREATED' ) {
                        INFO "File version $msg->{ file_version } was created.";
                        unless ( ws_is_next_file_exist($tx) ) {
                            INFO 'No more files to back up';
                            $tx->finish;
                        }
                    }
                    elsif ( $msg->{response} eq 'RESTORE_START_ACCEPTED' ) {
                        INFO 'Restore accepted.';
                        INFO Dumper @{ $msg->{ file_versions } };
                        @files_to_restore = @{ $msg->{ file_versions } };
                        ws_restore_next_file( $tx );
                    }
                    elsif ( $msg->{response} eq 'CHUNK') {
                        INFO "Chunks resieved $msg->{ chunk_number }.";
                        INFO( "File version $msg->{ file_version } is completed.") and $tx->finish unless defined $msg->{ chunk };
                        my $target = $args->{ '--target-id' };
                        $msg->{ file_version } =~ /^$target(.+)\/([0-9]+)$/;
                        my $path = $args->{ '-p' } . '/' . $2;
                        make_path $path;
                        $path .= $1;
                        $tx->finish unless $msg->{ chunk };
                        INFO $msg->{ file_version };
                        open ( my $fh, ">>$path" ) or die $!;
                        print $fh $msg->{ chunk };
                        close $fh;
                        ws_get_next_chunk( $tx, $msg );
                    }
                    elsif ( $msg->{response} eq 'LAST_CHUNK') {
                        INFO "Last chunk recieved.";
                        ws_restore_next_file( $tx );
                    }
                    else {
                        ERROR 'Unknown server response ' . Dumper $msg;
                        $tx->finish;
                    }
                }
            );

            my $command;
            if ( $args->{'-c'} eq 'backup' ) {
                $command = 'BACKUP_START';
            }
            elsif ( $args->{'-c'} eq 'restore' ) {
                $command = 'RESTORE_START';
            }
            $tx->send( j { request => $command, client => $args->{'-n'}, target => $args->{ '--target-id' } } );
        }
    );

    Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
}

