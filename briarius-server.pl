#!/usr/bin/env perl
use Mojolicious::Lite;

use DateTime;
use Data::Dumper;
use Digest::MurmurHash qw/murmur_hash/;
use File::Path qw(make_path remove_tree);
use File::stat;
use JSON::XS;
use Mojo::JSON 'j';
use Time::HiRes;
use Redis;

plugin 'PODRenderer';

use constant {
    FILES_REGISTRY      => 'files_registry/',
    CHUNCKS_REGISTRY    => 'chunks_registry/',
    CHUNCKS_USAGE       => 'chunks_usage/',
    INCOMPLETE_ENDING   => '.incomplete',
    FILE_VERSION_MARKER => '/.cafs_fv',

    BACKUP_FREQUENCY_THRESHOLD          => 60,
    CHUNK_SIZE                          => 64000,
    CHUNCKS_REGISTRY_BUCKET_NAME_LENGTH => 4,
    FILES_REGISTRY_BUCKET_SIZE          => 1024,
};

my $redis = Redis->new;
$redis->ping || die "Can not connect to Redis!";

my $started_at = DateTime->now;

sub last_number_in_dir {
    my $dirname = shift;
    opendir( my $dh, $dirname );
    my $res = 0;
    while ( readdir $dh ) {
        next unless /^[0-9]+$/;
        $res = $_ if $res < $_;
    }
    closedir $dh;
    return $res;
}

sub path_to_chunk {
    my $chunk_hash = shift;
    my @slices =
      grep { length $_ > 0 } split( /([0-9a-f]{2})/, $chunk_hash, 16 );
    pop @slices;
    return CHUNCKS_REGISTRY . join( '/' => @slices ) . '/' . $chunk_hash;
}

sub is_chunk_existed {
    my $chunk_hash = shift;
    my $bloom_filter = $redis->get('bloom_filter');
    return 0 if ($bloom_filter | murmur_hash($chunk_hash)) > $bloom_filter; 
    return -e path_to_chunk($chunk_hash);
}

sub create_file_version_key {
    my ( $user_name, $target_file_path, $time ) = @_;
    return join '/' => $user_name, $target_file_path, $time;
}

sub is_file_version_existed {
    return -e FILES_REGISTRY . shift;
}

sub is_available_for_backup {
    my $path_to_file_version = FILES_REGISTRY . shift;
    $path_to_file_version =~ /^(.+\/)[^\/]+$/;
    return 1 unless -e $1;
    my $last_backup_date = last_number_in_dir($1);
    return $last_backup_date + BACKUP_FREQUENCY_THRESHOLD <= time ? 1 : 0;
}

sub is_file_version_container {
    my $path_to = shift;
    return 0
      unless -e $path_to
      and -e $path_to . FILE_VERSION_MARKER
      and $path_to !~ /${\(INCOMPLETE_ENDING)}$/;
    return 0 if -f $path_to;
    return 1;
}

sub get_chunk {
    my ( $file_version, $chunk_number ) = @_;
    return 0 unless is_file_version_existed($file_version);
    my $bucket = int( $chunk_number / (FILES_REGISTRY_BUCKET_SIZE + 1) ) + 1;
    my $path_to_chunk =
      FILES_REGISTRY . $file_version . '/' . $bucket . '/' . $chunk_number;
    return undef unless -e $path_to_chunk;
    open( my $fh, '<', $path_to_chunk ) or return 0;
    my $data;
    my $n = read( $fh, $data, CHUNK_SIZE );
    close $fh;
    return $data;
}

sub get_all_file_versions {
    my $target_root = shift;
    return [] unless -e $target_root and -d $target_root;
    my @res;
    opendir( my $dh, $target_root );
    while ( readdir $dh ) {
        next if /^\./;
        my $path = "$target_root/$_";
        if ( -d $path and is_file_version_container $path ) {
            push @res, $path;
        }
        elsif ( -d $path ) {
            map { push @res, $_ } @{ get_all_file_versions($path) };
        }
    }
    closedir $dh;
    return \@res;
}

sub explore_tree {
    my $target_root = shift;
    return {} unless -e $target_root;
    say $target_root;
    my $res = {};
    opendir( my $dh, $target_root );
    while ( readdir $dh ) {
        next if /^\./;
        say;
        my $path = "$target_root/$_";
        say $path;
        my $size = -s;
        $res->{$path} = { size => $size };
        if ( -d $path and is_file_version_container $path ) {
            $res->{$path}->{type} = 'file';
        }
        else {
            $res->{$path}->{type}    = 'dir';
            $res->{$path}->{content} = explore_tree($path);
        }
    }
    closedir $dh;
    return $res;
}

sub on_message {
    my ( $self, $msg ) = @_;

    if ( length($msg) >= 500 ) {
        my $user     = 'Ivan';
        my $msg      = { grep { defined } split( '%%%%%%' => $msg ) };
        my $response = {};
        $response->{$_} = $msg->{$_} for qw/chunk_hash file file_version/;
        unless ( $redis->rpush( 'add_chunk_to_file_version', j $msg ) ) {
            $self->app->log->error(
                "Cant load chunk $msg->{ chunk_hash } to $msg->{ file_version }"
            );
            $response->{response} = 'CHUNK_WAS_NOT_LOADED';
        }
        else {
            $response->{response} = 'CHUNK_WAS_LOADED';
        }
        $self->send( j $response );
        return;
    }

    $msg = j $msg;

    if ( $msg->{request} eq 'BACKUP_START' ) {
        $self->app->log->info( 'Backup requested by ' . $msg->{client} );
        $self->send( j { response => 'BACKUP_START_ACCEPTED' } );

    }
    elsif ( $msg->{request} eq 'IS_FILE_VERSION_EXISTED' ) {
        $self->app->log->info(
"Client $msg->{ client } ask: is file version for file $msg->{ file } existed?"
        );
        my $user = 'Ivan';
        my $key = create_file_version_key( $user, $msg->{file}, time );
        unless ( is_available_for_backup($key) ) {
            $self->send(
                j {
                    response     => 'FILE_VERSION_IS_EXISTED',
                    file         => $msg->{file},
                    file_version => $key
                }
            );
        }
        else {
            $self->send(
                j {
                    response     => 'FILE_VERSION_IS_NOT_EXISTED',
                    file         => $msg->{file},
                    file_version => $key
                }
            );
        }
    }
    elsif ( $msg->{request} eq 'IS_CHUNK_EXISTED' ) {
        $self->app->log->info(
"Client $msg->{ client } ask: is chunk $msg->{ chunk_hash } for $msg->{ file_version } existed?"
        );
        my $user = 'Ivan';
        my $res  = {};
        $res->{$_} = $msg->{$_} for qw/chunk_hash file file_version/;
        if ( is_chunk_existed $msg->{chunk_hash} ) {
            $res->{response} = 'CHUNK_IS_EXISTED';
        }
        else {
            $res->{response} = 'CHUNK_IS_NOT_EXISTED';
        }
        $self->send( j $res );
    }
    elsif ( $msg->{request} eq 'ADD_EXISTED_CHUNK_TO_FILE_VERSION' ) {
        my $user     = 'Ivan';
        my $response = {};
        $response->{$_} = $msg->{$_} for qw/chunk_hash file file_version/;
        unless ( $redis->rpush( 'add_chunk_to_file_version', j $msg ) ) {
            $self->app->log->error("Cant add chunk to file");
            $response->{response} = 'CHUNK_WAS_NOT_LOADED';
        }
        else {
            $response->{response} = 'CHUNK_WAS_LOADED';
        }
        $self->send( j $response );
    }
    elsif ( $msg->{request} eq 'CREATE_FILE_VERSION' ) {
        my $response = {};
        $response->{$_} = $msg->{$_} for qw/chunk_hash file file_version/;
        if ( $redis->rpush( 'complete_file_version', j $msg ) ) {
            $response->{response} = 'FILE_VERSION_WAS_CREATED';
        }
        else {
            $response->{response} = 'FILE_VERSION_WAS_NOT_CREATED';
        }
        $self->send( j $response );
    }
    elsif ( $msg->{request} eq 'RESTORE_START' ) {
        $self->app->log->info( 'Restore requested by ' . $msg->{client} );
        my $user = 'Ivan';
        my @file_versions;
        for (
            @{
                get_all_file_versions(
                    FILES_REGISTRY . $user . '/' . $msg->{target}
                )
            }
          )
        {
            $_ =~ /^${\(FILES_REGISTRY)}$user\/(.+)$/;
            push @file_versions, $1;
        }

        $self->send(
            j {
                response      => 'RESTORE_START_ACCEPTED',
                file_versions => \@file_versions
            }
        );
    }
    elsif ( $msg->{request} eq 'GET_CHUNK' ) {
        $self->app->log->info('Chunk requested ');
        my $user = 'Ivan';
        my $chunk =
          get_chunk( $user . '/' . $msg->{file_version}, $msg->{chunk_number} );
        my $res = {};
        if ($chunk) {
            $res->{response}     = 'CHUNK';
            $res->{file_version} = $msg->{file_version};
            $res->{chunk_number} = $msg->{chunk_number};
            $res->{chunk}        = $chunk;
        }
        else {
            $res->{response} = 'LAST_CHUNK';
        }
        $self->send( j $res );
    }

}

sub on_finish {
    my ($self) = shift;
    $self->app->log->debug( "WebSocket closed with " . Dumper @_ );
}

get '/' => sub {
    my $self = shift;
    $self->render( json => { status => "Available since $started_at" } );
};

get '/tree' => sub {
    my $self   = shift;
    my $user   = 'Ivan';
    my $target = 'targetid43655645645634';
    my $tree = {};    #explore_tree FILES_REGISTRY . $user . '/' . $target;
    $self->render( json => $tree );
};

websocket '/pipe' => sub {
    my $self = shift;
    $self->app->log->debug('WebSocket opened.');
    $self->on( message => \&on_message );
    $self->on( finish  => \&on_finish );
};

app->start;
