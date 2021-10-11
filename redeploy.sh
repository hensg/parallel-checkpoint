#!/bin/bash

set -xe

user_id=hensg
ssh_client=${1}
ssh_list=${2}
IFS=',' read -ra ssh_list <<< "$ssh_list"

mvn clean package

scp target/BFT-SMaRt-parallel-cp-1.0-SNAPSHOT.jar ${user_id}@pc${ssh_client}.emulab.net:/srv/

for ssh_entry in "${ssh_list[@]}"
do
    scp target/BFT-SMaRt-parallel-cp-1.0-SNAPSHOT.jar ${user_id}@pc${ssh_entry}.emulab.net:/srv/
    ssh -p 22 ${user_id}@pc${ssh_entry}.emulab.net "sudo service bft-smart restart;"
done


