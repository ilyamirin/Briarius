Briarius
========

Prototype of file chunk store on Perl

I have to check my idea about a keeping simple file tree on cloud server directly on file system.

Just imagine: we have three b-tree - database index, database storage and file system. Can i use only last one? There was much work to check it on Java and i decided to write prototype on Perl (as usual).

It uses Redis (Perl project must use Redis).

I you wanna to run it simply, just run `sh server-start.sh && sh housekeeper-start.sh` and then run `client-backup.sh` for backup and `sh client-restore.sh` for restore)) 
