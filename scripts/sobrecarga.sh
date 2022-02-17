#!/bin/bash


set -e
set -o pipefail

user_id=hensg
ssh_client=${1}
ssh_list=${2}
IFS=',' read -ra ssh_list <<< "$ssh_list"

#client threads
num_threads=50
#num operations per client thread
num_ops=25000

datetime=$(date +%F_%H-%M-%S)

function start_experiment() { 
    local partitioned=$1
    local client_num_threads=$2
    local client_num_operations=$3
    local run=$4
    local server_threads=$5
    local client_interval=0
    local client_max_index=3
    local client_p_read=50
    local client_p_conflict=$6
    local client_verbose=false
    local client_parallel=true
    local client_async=false
    local checkpoint_interval=$7

    echo "Ensure client is not running"
    client_cmd="
        sudo killall -9 /usr/bin/java;
    "
    ssh -p 22 -o TCPKeepAlive=yes -o ServerAliveInterval=60 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_client}.emulab.net $client_cmd &

    echo "Starting experiments, reconfiguring service's replica"
    reconfigure_cmd="
        sudo service bft-smart stop;
        sudo sed -i s/'=PARTITIONED=.*'/=PARTITIONED=${partitioned}/g /etc/systemd/system/bft-smart.service;
        sudo sed -i s/'=PARALLEL=.*'/=PARALLEL=${partitioned}/g /etc/systemd/system/bft-smart.service;
        sudo sed -i s/'=THREADS=.*'/=THREADS=${server_threads}/g /etc/systemd/system/bft-smart.service;
        sudo sed -i s/'=CHECKPOINT_INTERVAL=.*'/=CHECKPOINT_INTERVAL=${checkpoint_interval}/g /etc/systemd/system/bft-smart.service;
        sudo rm -f /srv/config/currentView
        sudo rm -rf /disk*/checkpoint*/metadata/* /disk*/checkpoint*/states/*;
        sudo rm -rf /srv/logs/*.log ;
        sudo systemctl daemon-reload;
    "

    for ssh_entry in "${ssh_list[@]}"; do
         echo "Reconfiguring $ssh_entry"
         ssh -p 22 -o TCPKeepAlive=yes -o ServerAliveInterval=60 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_entry}.emulab.net "$reconfigure_cmd" &
    done
    wait
    for ssh_entry in "${ssh_list[@]}"; do
         ssh -p 22 -o TCPKeepAlive=yes -o ServerAliveInterval=60 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_entry}.emulab.net "sudo service bft-smart start;" &
    done
    wait 

    echo "Services reconfigured with paralell $partitioned and checkpoint interval $checkpoint_interval"

    echo "Starting client requests"
    client_cmd="
         sudo truncate -s 0 /srv/logs/*.log;
         tail -f /srv/logs/client.log &;
         cd /srv;
         sudo /usr/bin/java -cp /srv/BFT-SMaRt-parallel-cp-1.0-SNAPSHOT.jar demo.bftmap.BFTMapClientMP $client_num_threads 1 $client_num_operations $client_interval $client_max_index $client_p_read $client_p_conflict $client_verbose $client_parallel $client_async;
         kill %1;
    "
    ssh -p 22 -o TCPKeepAlive=yes -o ServerAliveInterval=60 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_client}.emulab.net $client_cmd

    echo "Client finished sending requests"

    #echo "Removing state/simulating crash"
    #recovery_mch=${ssh_list[0]}
    #recovery_cmd="
    #    rm -f /disk*/checkpoint*/states/*.ser;
    #    rm -f /disk*/checkpoint*/metadata/*.txt;
    #    rm -f /srv/config/currentView;
    #    sudo truncate -s 0 /srv/logs/*.log;
    #    sudo service bft-smart restart;
    #"
    #ssh -p 22 -o StrictHostKeyChecking=no ${user_id}@pc${recovery_mch}.emulab.net $recovery_cmd
    #sleep 3

    #echo "Starting client requests for recovery after server crash simulation"
    #client_cmd="
    #     sudo truncate -s 0 /srv/logs/*.log;
    #     tail -f /srv/logs/client.log &;
    #     cd /srv;
    #     sudo /usr/bin/java -cp /srv/BFT-SMaRt-parallel-cp-1.0-SNAPSHOT.jar demo.bftmap.BFTMapClientMP $client_num_threads 1 $client_num_operations $client_interval $client_max_index $client_p_read $client_p_conflict $client_verbose $client_parallel $client_async;
    #     kill %1;
    #"
    #ssh -p 22 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_client}.emulab.net $client_cmd &
    #wait

    echo "Getting remote logs"

    local experiment_dir=experiments/name=sobrecarga/datetime=$datetime/checkpoint=$checkpoint_interval/server_threads=$server_threads/clients=$num_threads/partitioned=${partitioned}/run=$run/read=${client_p_read}/conflict=${client_p_conflict}
    mkdir -p $experiment_dir

    scp ${user_id}@pc${ssh_client}.emulab.net:/srv/logs/client.log $experiment_dir/client.log &
    scp ${user_id}@pc${ssh_client}.emulab.net:/srv/logs/client_latency.log $experiment_dir/client_latency.log &
    for ssh_entry in "${ssh_list[@]}"; do
         scp ${user_id}@pc${ssh_entry}.emulab.net:/srv/logs/throughput.log $experiment_dir/throughput_$ssh_entry.log &
         scp ${user_id}@pc${ssh_entry}.emulab.net:/srv/logs/server.log $experiment_dir/server_$ssh_entry.log &
         scp ${user_id}@pc${ssh_entry}.emulab.net:/srv/logs/stdout.log $experiment_dir/stdout$ssh_entry.log &
    done
    wait 
    echo "Logs copied to $experiment_dir folder"
    echo "Experiment has finished"
}

#for checkpoint_interval in 300000 200000 100000; do
#    for particoes in 4 8 16; do
#        for conflito in 0 5 50 100; do 
for checkpoint_interval in 700000; do
    for particoes in 4; do
        for conflito in 0 5; do 
            start_experiment true $num_threads $num_ops 1 $particoes $conflito $checkpoint_interval;
            start_experiment false $num_threads $num_ops 1 $particoes $conflito $checkpoint_interval;
        done
    done
done
