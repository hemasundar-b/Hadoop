#!/bin/bash

host_list=$1
user=$2
pass=$3

## Create private/public keys in all servers listed
for i in $(cat $host_list)
do
host=$i
sshpass -p $pass ssh -o StrictHostKeyChecking=no $user@$host rm -rf $HOME/.ssh
cmd="sshpass -p $pass ssh -o StrictHostKeyChecking=no $user@$host ssh-keygen -t rsa"
expect <<-EOF
        spawn $cmd
        expect {
        "*yes*" { send "\r" ; exp_continue }
        "*id_rsa*" { send "\r" ; exp_continue }
        "*passphrase*" { send "\r" ; exp_continue }
        "*again*" { send "\r" ; exp_continue }
        }
EOF
done

## Place the public key for every server to all other servers
## Loop through all servers in the host list file

for i in $(cat $host_list)
do
## Loop through all servers in the host list file EXCEPT the current server being looked at in the main loop (variable i)
        for j in $(cat $host_list | grep -v $i )
        do
                ### scp id_rsa.pub over to remote host
                cmd="scp $user@${i}:$HOME/.ssh/id_rsa.pub $user@${j}:.ssh/id_rsa-${i}.pub"
                expect <<-EOF
                        spawn $cmd
                        expect {
                        "*assword*"   { send $pass\r; exp_continue }
                        "*yes/no*"    { send "yes\r"; exp_continue }
                        "*assword*"  { send "$pass\r" ; exp_continue}
                        }
                EOF
        done
done

for i in $(cat $host_list)
do
sshpass -p $pass ssh $user@$i cat "$HOME/.ssh/id_rsa-*" ">>" "$HOME/.ssh/authorized_keys"
done
