#!/bin/bash


set -e
set -o pipefail

user_id=hensg
ssh_client=${1}
ssh_list=${2}
IFS=',' read -ra ssh_list <<< "$ssh_list"


#Client execution config
threads=4
num_ops=3000

#Server config
checkpoint_interval=10000

datetime=$(date +%F_%H-%M-%S)

function start_experiment() { 
    local partitioned=$1
    local client_num_threads=$2
    local client_num_operations=$3
    local run=$4
    local client_interval=0
    local client_max_index=3
    local client_p_read=0
    local client_p_conflict=0
    local client_verbose=false
    local client_parallel=true
    local client_async=false

    echo "Starting experiments, reconfiguring service's replica"
    reconfigure_cmd="
        sudo service bft-smart stop;
        sudo sed -i s/'=PARTITIONED=.*'/=PARTITIONED=${partitioned}/g /etc/systemd/system/bft-smart.service;
        sudo sed -i s/'=PARALLEL=.*'/=PARALLEL=${partitioned}/g /etc/systemd/system/bft-smart.service;
        sudo sed -i s/'=CHECKPOINT_INTERVAL=.*'/=CHECKPOINT_INTERVAL=${checkpoint_interval}/g /etc/systemd/system/bft-smart.service;
        sudo sed -i s/'=NUM_DISK=.*'/=NUM_DISKS=1/g /etc/systemd/system/bft-smart.service;
        sudo rm -f /srv/config/currentView
        sudo rm -rf /disk*/checkpoint*/metadata/* /disk*/checkpoint*/states/*;
        sudo rm -rf /srv/logs/*.log ;
        sudo systemctl daemon-reload;
        sudo service bft-smart start;
    "

    for ssh_entry in "${ssh_list[@]}"; do
         echo "Reconfiguring $ssh_entry"
         ssh -p 22 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_entry}.emulab.net "$reconfigure_cmd"
    done

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

    local experiment_dir=experiments/$datetime/partitioned=${partitioned}/run=$run/read=${client_p_read}/conflict=${client_p_conflict}
    mkdir -p $experiment_dir
    for ssh_entry in "${ssh_list[@]}"; do
         scp ${user_id}@pc${ssh_entry}.emulab.net:/srv/logs/throughput.log $experiment_dir/throughput_$ssh_entry.log &
         scp ${user_id}@pc${ssh_entry}.emulab.net:/srv/logs/server.log $experiment_dir/server_$ssh_entry.log &
    done
    wait 
    echo "Logs copied to $experiment_dir folder"
    echo "Experiment has finished"
}


#for i in $(seq 1 1); do start_experiment false $threads $num_ops $i; done
for i in $(seq 1 1); do start_experiment true $threads $num_ops $i; done
