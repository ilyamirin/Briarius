#!/usr/bin/env perl
use Mojolicious::Lite;

use DateTime;
use Mojo::JSON 'j';

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

sub create_empty_file_version {
	my ( $self, $user_name, $target_name, $file_name, $time ) = @_;
	
	my $key = create_file_version_key( $user_name, $target_name, $file_name, $time );
	
	unless ( -e FILES_REGISTRY . $key . INCOMPLETE_ENDING ) {
		$self->app->log->error( "Incomplete file version for $key has not been created yet!" );
		return 0;
	}

	$self->app->log->info( "Create file version: $key" );
	rename FILES_REGISTRY . $key . INCOMPLETE_ENDING => FILES_REGISTRY . $key;	
	return 1;
}

sub add_chunk_to_version {
	my ( $self, $user_name, $target_name, $file_name, $time, $chunk_hash ) = @_;
	my $key = create_file_version_key( $user_name, $target_name, $file_name, $time );
	$self->app->log->info( "Start chunk to file version addition. Chunk hash: $chunk_hash, file version: $key" );
    my $file;
    open( $file, '>>', FILES_REGISTRY . $key . INCOMPLETE_ENDING ) or $self->app->log->error( $! ) and return 0;
    print "$chunk_hash\n", $file;
    close $file;
    return 1;
}

sub is_chunk_existed {
	my ( $self, $chunk_hash ) = @_;
	return -e CHUNCKS_REGISTRY . $chunk_hash;
}

sub create_chunk {
	my ( $self, $chunk_hash, $chunk_content ) = @_;
	if ( is_chunk_existed $chunk_hash ) {
		$self->app->log->error( "Chunck with hash $chunk_hash has already been created!" );
		return 0;
	}
	$self->app->log->info( "Chunk with hash $chunk_hash is backuping now" );
    my $file;
    open( $file, '>>', CHUNCKS_REGISTRY . $chunk_hash ) or $self->app->log->error( $! ) and return 0;
    print $chunk_content, $file;
    close $file;	
	return 1;
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
		my $key = create_file_version_key( $user, $msg->{ file }, time() );
		if ( is_file_version_existed $key ) {
			$self->send( j { response => 'FILE_VERSION_IS_EXISTED', file => $msg->{ file }, file_version => $key } );
		}
		else {
			$self->send( j { response => 'FILE_VERSION_IS_NOT_EXISTED', file => $msg->{ file }, file_version => $key } );
		}
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
