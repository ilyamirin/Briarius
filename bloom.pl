#!/usr/bin/env perl 
use Modern::Perl;
use Digest::MD5 qw/md5/;
use Digest::MurmurHash qw/murmur_hash/;

my $bloom_filter = 0;
my @values = ( 'test', 'testosterone' );

$bloom_filter |= murmur_hash($_) for @values;
say $bloom_filter;

while (<>) { 
    chomp;
    if (($bloom_filter | murmur_hash($_)) > $bloom_filter) {
        say 'NO';
    }
    else {
        say 'MAY BE';
    }
}
