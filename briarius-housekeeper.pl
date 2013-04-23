use Data::Dumper;
use Digest::MurmurHash qw/murmur_hash/;
use File::Path qw(make_path remove_tree);
use Log::Log4perl qw(:easy);
use Mojo::JSON 'j';
use Redis;

use constant {
    FILES_REGISTRY             => 'files_registry/',
    CHUNCKS_REGISTRY           => 'chunks_registry/',
    INCOMPLETE_ENDING          => '.incomplete',
    FILE_VERSION_MARKER        => '/.cafs_fv',
    CHUNK_SIZE                 => 64000,
    FILES_REGISTRY_BUCKET_SIZE => 1024,
};

my $redis = Redis->new;
$redis->ping || die "Can not connect to Redis!";

sub last_number_in_dir {
    my $dirname = shift;
    INFO "LND: $dirname";
    opendir( my $dh, $dirname );
    $res = 0;
    while ( readdir $dh ) {
        next if /^\./;
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

sub create_chunk {
    my ( $chunk_hash, $chunk_content ) = @_;
    my $path = path_to_chunk($chunk_hash);
    #return 1 if -e $path;
    $path =~ /^(.+\/)[^\/]+$/;
    make_path($1);# unless -e $1;
    open( my $file, '>', $path ) or return 0;
    print $file $chunk_content;
    close $file;
    my $bloom_filter = $redis->get('bloom_filter') or 0;
    $bloom_filter |= murmur_hash($chunk_hash); 
    $redis->set('bloom_filter', $bloom_filter);
    return 1;
}

sub add_chunk_to_version {
    my ( $chunk_hash, $file_version ) = @_;
    my $path_to_file = FILES_REGISTRY . $file_version . INCOMPLETE_ENDING;
    #unless ( -e $path_to_file ) {
        make_path $path_to_file;
        open( my $fh, '>', $path_to_file . FILE_VERSION_MARKER );
        print $fh ' ';
        close $fh;
        #}
    my $last_bucket = $redis->get("last_number_in_$path_to_file") or last_number_in_dir($path_to_file);
    unless ($last_bucket) {
        $last_bucket = 1;
        make_path( $path_to_file . '/' . $last_bucket );
        $redis->set("last_number_in_$path_to_file", 1);
    }
    my $dirname = $path_to_file . '/' . $last_bucket;
    my $last_chunk_number = $redis->get("last_number_in_$dirname");
    unless ($last_chunk_number > 0) {
        $redis->set("last_number_in_$dirname", 0);
        $last_chunk_number = last_number_in_dir($dirname);
    }
    $last_chunk_number++;
    if ( $last_chunk_number > FILES_REGISTRY_BUCKET_SIZE * $last_bucket ) {
        $last_bucket++;
        $redis->incr("last_number_in_$path_to_file");
        make_path $path_to_file . '/' . $last_bucket;
    }
    $redis->incr("last_number_in_$dirname");
    link path_to_chunk($chunk_hash),
      $path_to_file . '/' . $last_bucket . '/' . $last_chunk_number;
}

sub is_file_version_existed {
    return -e FILES_REGISTRY . shift;
}

sub complete_file_version {
    my ($file_version) = @_;
    return 0 if is_file_version_existed $file_version;
    my $path_to_version            = FILES_REGISTRY . $file_version;
    my $path_to_incomplete_version = $path_to_version . INCOMPLETE_ENDING;
    return 0 unless -e $path_to_incomplete_version;
    return rename $path_to_incomplete_version, $path_to_version;
}

sub main {
    while ( my $msg = j $redis->lpop('add_chunk_to_file_version') ) {
        INFO "Chunk $msg->{ chunk_hash } recieved.";
        my $is_chunk_created = create_chunk($msg->{chunk_hash}, $msg->{chunk}); 
        if ( $is_chunk_created ) {
            INFO "Chunk $msg->{ chunk_hash } was created.";
            my $is_chunk_added = add_chunk_to_version($msg->{chunk_hash}, $msg->{file_version});
            if ($is_chunk_added) {
                INFO "Chunk $msg->{ chunk_hash } was added to $msg->{ file_version }.";
            }
            else {
                ERROR "Chunk $msg->{ chunk_hash } was NOT added to $msg->{ file_version }.";
            }
        }
        else {
            ERROR "Chunk $msg->{ chunk_hash } was NOT created.";
        }
    }

    while ( my $msg = j $redis->lpop('complete_file_version') ) {
        INFO "Complete file version $msg->{ file_version } recieved.";
        if ( complete_file_version( $msg->{file_version} ) ) {
            INFO "File version $msg->{ file_version } was completed.";
        }
        else {
            ERROR "File version $msg->{ file_version } was NOT completed.";
        }
    }
}    #main

Log::Log4perl->easy_init(
    {
        level  => 'DEBUG',
        file   => "STDOUT",
        layout => '%m%n'
    },
);

INFO 'Housekeeper just has been started.';

main() while 1;

INFO 'Housekeeper has his deal done and now he has to leave.';

