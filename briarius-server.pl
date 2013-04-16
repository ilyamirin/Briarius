#!/usr/bin/env perl
use Mojolicious::Lite;

use DateTime;
use Mojo::JSON 'j';

plugin 'PODRenderer';

my $started_at = DateTime->now;

sub on_message {
	my ( $self, $msg ) = @_;
	
	$msg = j $msg;
	
	if ( $msg->{ request } eq 'BACKUP_START' ) {
		$self->app->log->info('Backup requested');
	}

	$self->send( j { response => 'BACKUP_START_ACCEPTED' } );
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
