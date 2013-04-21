#!/usr/bin/env perl
use Mojolicious::Lite;

use DateTime;
use Data::Dumper;
use File::Path qw(make_path remove_tree);
use File::stat;
use Mojo::JSON 'j';
use Time::HiRes;

plugin 'PODRenderer';

use constant {
	FILES_REGISTRY 	  => 'files_registry/',
	CHUNCKS_REGISTRY  => 'chunks_registry/',
	CHUNCKS_USAGE     => 'chunks_usage/',
	INCOMPLETE_ENDING => '.incomplete',
	FILE_VERSION_MARKER => '/.cafs_fv',

	BACKUP_FREQUENCY_THRESHOLD          => 60,
	CHUNK_SIZE                          => 64000,
	CHUNCKS_REGISTRY_BUCKET_NAME_LENGTH => 4,
	FILES_REGISTRY_BUCKET_SIZE          => 1024,
}; 

my $started_at = DateTime->now;

sub last_number_in_dir {
	my $dirname = shift;
	opendir( my $dh, $dirname );
	my $res = 0;
	while ( readdir $dh ) {
		next if /^\./;
		$res = $_ if $res < $_;
	}
    closedir $dh;
    return $res; 
}

sub path_to_chunk {
	my $chunk_hash = shift;
	my @slices = grep { length $_ > 0 } split( /([0-9a-f]{2})/ , $chunk_hash, 16 );
	pop @slices;
	return CHUNCKS_REGISTRY . join( '/' =>  @slices ) . '/' . $chunk_hash;
}

sub add_chunk_to_version {
	my ( $chunk_hash, $file_version ) = @_;
	my $path_to_file = FILES_REGISTRY . $file_version . INCOMPLETE_ENDING;
	unless( -e $path_to_file ) {
		make_path $path_to_file;
		open( my $fh, '>', $path_to_file . FILE_VERSION_MARKER); 
		print $fh ' ';
		close $fh;
	}
	my $last_bucket = last_number_in_dir( $path_to_file );	
	unless ( $last_bucket ) {
		$last_bucket = 1;
		make_path( $path_to_file . '/' . $last_bucket );
	}
	my $last_chunk_number = last_number_in_dir( $path_to_file . '/' . $last_bucket ) + 1;
	if ( $last_chunk_number > FILES_REGISTRY_BUCKET_SIZE * $last_bucket ) {
		$last_bucket++;
		make_path $path_to_file . '/' . $last_bucket;
	}
	link path_to_chunk( $chunk_hash ), $path_to_file . '/' . $last_bucket . '/' . $last_chunk_number;
}

sub is_chunk_existed {
	return -e path_to_chunk( shift );
}

sub create_chunk {
	my ( $chunk_hash, $chunk_content ) = @_;
	my $path = path_to_chunk( $chunk_hash );
	$path =~ /^(.+\/)[^\/]+$/;
	make_path( $1 ) unless -e $1;
    open( my $file, '>', $path ) or return 0;
    print $file $chunk_content;
    close $file;
	return 1;
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
	my $last_backup_date = last_number_in_dir( $1 );
    return $last_backup_date + BACKUP_FREQUENCY_THRESHOLD <= time ? 1 : 0;
}

sub complete_file_version {
	my ( $file_version ) = @_;
	return ( 0, "File version $file_version is already existed" ) if is_file_version_existed $file_version;
	my $path_to_version = FILES_REGISTRY . $file_version;
	my $path_to_incomplete_version = $path_to_version . INCOMPLETE_ENDING;
	return ( 0, "Has no incomplete $file_version" ) unless -e $path_to_incomplete_version;
	return rename $path_to_incomplete_version, $path_to_version;
}

sub is_file_version_container {
	my $path_to = shift;
	return 0 unless -e $path_to and -e $path_to . FILE_VERSION_MARKER and $path_to !~ /${\(INCOMPLETE_ENDING)}$/;
	return 0 if -f $path_to;
	return 1;
}

sub get_chunk {
	my ( $file_version, $chunk_number ) = @_;
	return 0 unless is_file_version_existed( $file_version );
	my $bucket = int( $chunk_number / FILES_REGISTRY_BUCKET_SIZE );
	$bucket++ if $chunk_number != FILES_REGISTRY_BUCKET_SIZE;
	my $path_to_chunk = FILES_REGISTRY . $file_version . '/' . $bucket . '/' . $chunk_number;
	say $path_to_chunk;
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
			map { push @res, $_ } @{ get_all_file_versions( $path ) };
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
		$res->{ $path } = { size => $size };
		if ( -d $path and is_file_version_container $path ) {
			$res->{ $path }->{ type } = 'file';
		}
		else {
			$res->{ $path }->{ type } = 'dir';
			$res->{ $path }->{ content } = explore_tree( $path );
		}
	}
	closedir $dh;
	return $res;
}

sub on_message {
	my ( $self, $msg ) = @_;
	
	$msg = j $msg;
	
	if ( $msg->{ request } eq 'BACKUP_START' ) {
		$self->app->log->info( 'Backup requested by ' . $msg->{ client } );
		$self->send( j { response => 'BACKUP_START_ACCEPTED' } );

	}
	elsif ( $msg->{ request } eq 'IS_FILE_VERSION_EXISTED' ) {
		$self->app->log->info( "Client $msg->{ client } ask: is file version for file $msg->{ file } existed?" );
		my $user = 'Ivan';
		my $key = create_file_version_key( $user, $msg->{ file }, time );
		unless ( is_available_for_backup( $key ) ) {			
			$self->send( j { response => 'FILE_VERSION_IS_EXISTED', file => $msg->{ file }, file_version => $key } );
		}
		else {
			$self->send( j { response => 'FILE_VERSION_IS_NOT_EXISTED', file => $msg->{ file }, file_version => $key } );
		}
	}
	elsif ( $msg->{ request } eq 'IS_CHUNK_EXISTED' ) {
		$self->app->log->info( "Client $msg->{ client } ask: is chunk $msg->{ chunk_hash } for $msg->{ file_version } existed?" );
		my $user = 'Ivan';
		my $res = {};
		$res->{ $_ } = $msg->{ $_ } for qw/chunk_hash file file_version/;
		if ( is_chunk_existed $msg->{ chunk_hash } ) {			
			$res->{ response } = 'CHUNK_IS_EXISTED';
		}
		else {
			$res->{ response } = 'CHUNK_IS_NOT_EXISTED';
		}
		$self->send( j $res );
	}
	elsif ( $msg->{ request } eq 'LOAD_CHUNK' ) {
		my $user = 'Ivan';
		my $response = {};
		$response->{ $_ } = $msg->{ $_ } for qw/chunk_hash file file_version/;
		unless ( create_chunk $msg->{ chunk_hash }, $msg->{ chunk } ) {
			$self->app->log->error( "Cant load chunk $msg->{ chunk_hash }" ); 
			$response->{ response } = 'CHUNK_WAS_NOT_LOADED';
		}
		else {
			$response->{ response } = 'CHUNK_WAS_LOADED';			
			unless ( add_chunk_to_version $msg->{ chunk_hash }, $msg->{ file_version } ) {
				$self->app->log->error( "Cant add chunk to file" ); 
				$response->{ response } = 'CHUNK_WAS_NOT_LOADED';
			}
		}
		$self->send( j $response );
	}
	elsif ( $msg->{ request } eq 'ADD_EXISTED_CHUNK_TO_FILE_VERSION' ) {		
		my $user = 'Ivan';
		my $response = {};
		$response->{ $_ } = $msg->{ $_ } for qw/chunk_hash file file_version/;		
		unless ( add_chunk_to_version $msg->{ chunk_hash }, $msg->{ file_version } ) {
			$self->app->log->error( "Cant add chunk to file" ); 
			$response->{ response } = 'CHUNK_WAS_NOT_LOADED';
		} 
		else {
			$response->{ response } = 'CHUNK_WAS_LOADED';
		}
		$self->send( j $response );
	}
	elsif ( $msg->{ request } eq 'CREATE_FILE_VERSION' ) {
		my $response = {};
		$response->{ $_ } = $msg->{ $_ } for qw/chunk_hash file file_version/;
		my ( $status, $message ) = complete_file_version $msg->{ file_version };		
		if ( $status ) {
			$response->{ response } = 'FILE_VERSION_WAS_CREATED';
		}
		else {
			$self->app->log->error( $message );
			$response->{ response } = 'FILE_VERSION_WAS_NOT_CREATED';
		}
		$self->send( j $response );
	}
	elsif ( $msg->{ request } eq 'RESTORE_START' ) {
		$self->app->log->info( 'Restore requested by ' . $msg->{ client } );
		my $user = 'Ivan';
		my @file_versions;
		for ( @{ get_all_file_versions( FILES_REGISTRY . $user . '/' . $msg->{ target } ) } ) {
			$_ =~ /^${\(FILES_REGISTRY)}$user\/(.+)$/;
			push @file_versions, $1;
		}

		$self->send( j { response => 'RESTORE_START_ACCEPTED', file_versions => \@file_versions } );
	}
	elsif ( $msg->{ request } eq 'GET_CHUNK' ) {
		$self->app->log->info( 'Chunk requested ' );
		my $user = 'Ivan';
		my $chunk = get_chunk( $user . '/' . $msg->{ file_version }, $msg->{ chunk_number } );
		my $res = {};
		$res->{ response } = 'CHUNK';
		$res->{ file_version } = $msg->{ file_version };
		$res->{ chunk_number } = $msg->{ chunk_number };
		$res->{ chunk } = $chunk;
		$self->send( j $res );
	}

}

sub on_finish {
	my ( $self ) = shift;
	$self->app->log->debug("WebSocket closed with " . Dumper @_);
}

get '/' => sub {
  my $self = shift;
  $self->render( json => { status => "Available since $started_at" } );
};

get '/tree' => sub {
	my $self = shift;
	my $user = 'Ivan';
	my $target = 'targetid43655645645634';	
	my $tree = {};#explore_tree FILES_REGISTRY . $user . '/' . $target;
	$self->render( json => $tree );
};

websocket '/pipe' => sub {
	my $self = shift;
	$self->app->log->debug('WebSocket opened.');
	$self->on( message => \&on_message );
	$self->on( finish => \&on_finish );
};

app->start;
