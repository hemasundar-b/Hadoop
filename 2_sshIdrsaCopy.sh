#!/bin/bash
host_list=$1
user=$2
pass=$3

## Place the public key for every server to all other servers
## Loop through all servers in the host list file

for i in $(cat $host_list)
do
## Loop through all servers in the host list file EXCEPT the current server being looked at in the main loop (variable i)
        for j in $(cat $host_list | grep -v $i )
        do
                ### scp id_rsa.pub over to remote host
                cmd="scp $user@${i}:$HOME/.ssh/id_rsa.pub $user@${j}:.ssh/id_rsa-${i}.pub"
                expect <<- DONE
                spawn $cmd
                expect {
                "*assword*"   { send "$pass\r"; exp_continue }
                "*yes/no*"    { send "yes\r"; exp_continue }
                "*assword*"  { send "$pass\r" ; exp_continue}
                }
                DONE
        done
done
