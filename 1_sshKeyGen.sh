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
        expect <<-DONE
        spawn $cmd
        expect {
                "*id_rsa*" { send \r ; exp_continue }
                "*passphrase*" { send \r ; exp_continue }
                "*again*" { send \r ; exp_continue }
        }
        DONE
done
