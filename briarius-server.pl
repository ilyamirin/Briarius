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
	INCOMPLETE_ENDING => '.incomplete'
};

my $started_at = DateTime->now;

sub create_file_version_key {
	my ( $user_name, $target_file_path, $time ) = @_;
	return join '/' => $user_name, $target_file_path, $time;
}

sub is_file_version_existed {
	return -e FILES_REGISTRY . shift;
}

sub add_chunk_to_version {
	my ( $chunk_hash, $file_version ) = @_;
	$file_version =~ /^(.+)\/[^\/]+$/;
	make_path FILES_REGISTRY . $1;
	say FILES_REGISTRY . $file_version . INCOMPLETE_ENDING;
    open( my $file, '>>', FILES_REGISTRY . $file_version . INCOMPLETE_ENDING ) or return 0;
    print $file "$chunk_hash\n";
    close $file;
    return 1;
}

sub is_chunk_existed {
	return -e CHUNCKS_REGISTRY . shift;
}

sub create_chunk {
	my ( $chunk_hash, $chunk_content ) = @_;
	if ( -e CHUNCKS_REGISTRY . $chunk_hash ) {
		return 0;
	}
    $chunk_hash =~ s/\//--/g;
    open( my $file, '>', CHUNCKS_REGISTRY . $chunk_hash ) or return 0;
    print $file $chunk_content;
    close $file;
	return 1;
}

sub create_file_version {
	my ( $file_version ) = @_;
	my $path_to_version = FILES_REGISTRY . $file_version;
	my $path_to_incomplete_version = $path_to_version . INCOMPLETE_ENDING;
	return ( 0, "File version $file_version is already existed" ) if -e $path_to_version;
	return ( 0, "Has no incomplete $file_version" ) unless -e $path_to_incomplete_version;
	return rename $path_to_incomplete_version, $path_to_version;
}

sub on_message {
	my ( $self, $msg ) = @_;
	
	$msg = j $msg;
	
	if ( $msg->{ request } eq 'BACKUP_START' ) {
		$self->app->log->info('Backup requested by ' . $msg->{ client });
		$self->send( j { response => 'BACKUP_START_ACCEPTED' } );

	}
	elsif ( $msg->{ request } eq 'IS_FILE_VERSION_EXISTED' ) {
		$self->app->log->info( "Client $msg->{ client } ask: is file version for file $msg->{ file } existed?" );

		my $user = 'Ivan';
		my $key = create_file_version_key( $user, $msg->{ file }, time );
		if ( is_file_version_existed $key ) {
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
		my ( $status, $message ) = create_file_version $msg->{ file_version };		
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
	my ( $self, $code, $reason ) = @_;
	$self->app->log->debug("WebSocket closed with status $code.");
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
