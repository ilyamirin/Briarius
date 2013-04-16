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

my $args;

sub process_file {
    my ( $path_to_file, $tx ) = @_;

    if ( -d $path_to_file ) {
        INFO 'Directory has been found: ' . $path_to_file;

        my $dir;
        opendir $dir, $path_to_file or die $!;

        while ( readdir $dir ) {
            next if $_ =~ /^\.\.?$/;
            $path_to_file .= '/' unless $path_to_file =~ /\/$/;
            process_file( $path_to_file . $_ , $tx );
        }

        closedir $dir;

    } elsif ( -f $path_to_file ) {
        INFO 'File has been found: ' . $path_to_file;        

        #TODO:: can i backup file?
        #$tx->send( j { request => 'IS_FILE_VERSION_EXIST', client => $args->{ '-n' } } );

        my $file;
        open $file, "<", $path_to_file or die $!;

        flock $file, 1;

        my ( $buf, $data, $n );
        while ( ( $n = read $file, $data, CHUNK_SIZE ) > 0 ) {
            INFO "$n bytes hav been readed.";
            $buf .= $data;
        }

        close $file;

    }#elsif

}#sub process_file

my @files = ();

sub get_target_path {
    my ( $absolute_path, $outer_path ) = @_;
    if ( $outer_path =~ /^$absolute_path\/(.+)$/ ) {
        return $1;
    }
    else {
        ERROR "Incorrect path $absolute_path, $outer_path";
    }
}

sub crawl {
    my ( $path_to_file ) = @_;

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

    } elsif ( -f $path_to_file ) {
        INFO 'File has been found: ' . $path_to_file;    
        push @files, $path_to_file;    

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

	$args = { @ARGV };

	crawl $args->{ '-p' }; #crawl all files

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
                my $file = $args->{ '--target-id' } . '/' . get_target_path( $args->{ '-p' }, shift( @files ) );
                $tx->send( j { request => 'IS_FILE_VERSION_EXISTED', client => $args->{ '-n' }, file => $file } );

            } 
            elsif ( $msg->{ response } eq 'FILE_VERSION_IS_NOT_EXISTED' ) {
                INFO "File version $msg->{ file_version } is not existed";
                #start backup file
            }
            elsif ( $msg->{ response } eq 'FILE_VERSION_IS_EXISTED' ) {
                INFO "File version $msg->{ file_version } is existed";
                
            }
            else {
                ERROR "Unknown server response $msg";
                $tx->finish;
            }		
		});

        my $command = 'BACKUP_START' if $args->{ '-c' } eq 'backup';
		$tx->send( j { request => 'BACKUP_START', client => $args->{ '-n' } } );
	});

	Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
}



