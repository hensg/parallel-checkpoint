#!/bin/bash


set -e
set -o pipefail

user_id=hensg
ssh_client=${1}
ssh_list=${2}
IFS=',' read -ra ssh_list <<< "$ssh_list"

datetime=$(date +%F_%H-%M-%S)

#Client execution config
threads=4
num_ops=35000

#Server config
checkpoint_interval=50000

function start_experiment() { 
    local partitioned=$1
    local datetime=$2
    local client_num_threads=$3
    local client_num_operations=$4
    local client_interval=0
    local client_max_index=3
    local client_p_read=10
    local client_p_conflict=10
    local client_verbose=false
    local client_parallel=true
    local client_async=false
    echo "Starting experiments, reconfiguring service's replica"
    reconfigure_cmd="
        sudo service bft-smart stop;
        sudo sed -i s/'=PARTITIONED=.*'/=PARTITIONED=${partitioned}/g /etc/systemd/system/bft-smart.service;
        sudo sed -i s/'=CHECKPOINT_INTERVAL=.*'/=CHECKPOINT_INTERVAL=${checkpoint_interval}/g /etc/systemd/system/bft-smart.service;
        sudo rm /srv/config/currentView
        sudo rm -rf /checkpoint0/metadata/* /checkpoint0/states/*;
        sudo rm -rf /srv/logs/*.log ;
        sudo systemctl daemon-reload;
        sudo service bft-smart start;
    "

    for ssh_entry in "${ssh_list[@]}"; do
         ssh -p 22 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_entry}.emulab.net "$reconfigure_cmd" &
    done

    wait
    sleep 4s

    echo "Services reconfigured with paralell $partitioned and checkpoint interval $checkpoint_interval"

    echo "Starting client requests"
    client_cmd="
         sudo truncate -s 0 /srv/logs/*.log;
         tail -f /srv/logs/client.log &;
         cd /srv;
         sudo /usr/bin/java -cp /srv/BFT-SMaRt-parallel-cp-1.0-SNAPSHOT.jar demo.bftmap.BFTMapClientMP $client_num_threads 1 $client_num_operations $client_interval $client_max_index $client_p_read $client_p_conflict $client_verbose $client_parallel $client_async;
         kill %1;
    "
    ssh -p 22 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_client}.emulab.net $client_cmd

    echo "Client finished sending requests"
    echo "Stoping services and getting remote logs"

    local experiment_dir=experiments/$datetime/partitioned=${partitioned}
    mkdir -p $experiment_dir
    for ssh_entry in "${ssh_list[@]}"; do
         scp ${user_id}@pc${ssh_entry}.emulab.net:/srv/logs/throughput.log $experiment_dir/throughput_$ssh_entry.log &
         scp ${user_id}@pc${ssh_entry}.emulab.net:/srv/logs/server.log $experiment_dir/server_$ssh_entry.log &
    done
    wait 
    echo "Logs copied to $experiment_dir folder"
    echo "Experiment has finished"
}


start_experiment false $datetime $threads $num_ops
start_experiment true $datetime $threads $num_ops
