#!/usr/bin/env perl
use Mojolicious::Lite;

use DateTime;
use Data::Dumper;
use File::Path qw(make_path remove_tree);
use Mojo::JSON 'j';
#use Time::HiRes qw/ time sleep /;

plugin 'PODRenderer';

use constant {
	FILES_REGISTRY 	  => 'files_registry/',
	CHUNCKS_REGISTRY  => 'chunks_registry/',
	CHUNCKS_USAGE     => 'chunks_usage/',
	INCOMPLETE_ENDING => '.incomplete',

	BACKUP_FREQUENCY_THRESHOLD          => 60,
	CHUNK_SIZE                          => 64000,
	CHUNCKS_REGISTRY_BUCKET_NAME_LENGTH => 4,
	FILES_REGISTRY_BUCKET_SIZE          => 1024,
}; 

my $started_at = DateTime->now;

sub get_last_numeric_filename_into_dir {
	my $dirname = shift;
	opendir( my $dh, $dirname );
	my @numeric_filenames = sort { $a <=> $b } grep { /^[0-9]+$/ and -f "$dirname/$_" } readdir( $dh );
    closedir $dh;
    return scalar @numeric_filenames ? pop @numeric_filenames : 0; 
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
	make_path $path_to_file unless -e $path_to_file;;
	my $last_bucket = get_last_numeric_filename_into_dir( $path_to_file );	
	unless ( $last_bucket ) {
		$last_bucket++;
		make_path( $path_to_file . '/' . $last_bucket );
	}
	my $last_chunk_number = get_last_numeric_filename_into_dir( $path_to_file . '/' . $last_bucket );
	unless ( $last_chunk_number <= FILES_REGISTRY_BUCKET_SIZE) {
		$last_bucket++;
		make_path $path_to_file . '/' . $last_bucket;
	}
	link path_to_chunk( $chunk_hash ), $path_to_file . '/' . $last_bucket . '/' . ++$last_chunk_number;
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

sub get_chunk {
	my $path = path_to_chunk( shift );
	return 0 unless -e $path;
    open( my $file, '<', $path ) or return 0;
    flock $file, 1;
    my $data;
    my $n = read( $file, $data, CHUNK_SIZE );
    close $file;
	return $data;	
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
	my $last_backup_date = get_last_numeric_filename_into_dir( $1 );
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
}

sub on_finish {
	my ( $self ) = shift;
	$self->app->log->debug("WebSocket closed with " . Dumper @_);
}

get '/' => sub {
  my $self = shift;
  $self->render( json => { status => "Available since $started_at" } );
};

websocket '/pipe' => sub {
	my $self = shift;
	$self->app->log->debug('WebSocket opened.');
	$self->on( message => \&on_message );
	$self->on( finish => \&on_finish );
};

app->start;
