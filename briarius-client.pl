#!/usr/bin/env perl

use Modern::Perl;
use Data::Dumper;    
use Log::Log4perl qw(:easy);
use Mojo::UserAgent;
use Mojo::JSON 'j';

use constant {
    #working parameters
    CHUNK_SIZE => 64000,
};

sub process_file {
    my $file_handler = shift;

    if ( -d $file_handler ) {
        say 'Directory has been found: ' . $file_handler;

        my $dir;
        opendir $dir, $file_handler or die $!;

        while ( readdir $dir ) {
            next if $_ =~ /^\.\.?$/;
            $file_handler .= '/' unless $file_handler =~ /\/$/;
            process_file( $file_handler . $_ );
        }

        closedir $dir;

    } elsif ( -f $file_handler ) {
        say 'File has been found: ' . $file_handler;

        my $file;
        open $file, "<", $file_handler or die $!;

        flock $file, 1;

        my ( $buf, $data, $n );
        while ( ( $n = read $file, $data, CHUNK_SIZE ) > 0 ) {
            say "$n bytes hav been readed.";
            $buf .= $data;
         }

         close $file;

    }#elsif

}#sub process_file

BEGIN {
	Log::Log4perl->easy_init( 
#		{ 
#			level    => 'DEBUG',
#			file     => ">>briarius-client.log",
#			layout   => '%F{1}-%L-%M: %m%n' 
#		},
		{ 
			level    => 'DEBUG',
            file     => "STDOUT",
            layout   => '%m%n' 
        }, 
    );

	my $args = { @ARGV };

	#process_file $args->{ '-p' };

	my $ua = Mojo::UserAgent->new;

	$ua->websocket( $args->{ '-s' } => sub {
		my ( $ua, $tx ) = @_;
		
        ERROR 'WebSocket handshake failed!' and return unless $tx->is_websocket;
		
        $tx->on( finish => sub {
			my ( $tx, $code, $reason ) = @_;
			INFO "Servers session closed because $reason";
		});

		$tx->on( message => sub {
			my ( $tx, $msg ) = @_;

            $tx->finish and return unless $msg;
			
            DEBUG "Server response: $msg";

            $msg = j $msg;            
            if ( $msg->{ response } eq 'BACKUP_START_ACCEPTED' ) {
                INFO 'Backup start accepted by server';
            }
            else {
                ERROR "Unknown server response $msg";
                $tx->finish;
            }		
		});

        my $command = 'BACKUP_START' if $args->{ '-c' } eq 'backup';
		$tx->send( j { request => 'BACKUP_START' } );

	});

	Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
}



