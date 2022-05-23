#!/bin/bash


set -e
set -o pipefail

user_id=hensg
ssh_client=${1}
ssh_list=${2}
IFS=',' read -ra ssh_list <<< "$ssh_list"


datetime=$(date +%F_%H-%M-%S)

function start_experiment() { 
    local partitioned=$1
    local client_client_num_threads=$2
    local client_num_operations=$3
    local run=$4
    local server_threads=$5
    local client_interval=0
    local client_max_index=$server_threads    
    local client_p_conflict=$6
    local client_verbose=false
    local client_parallel=true
    local client_async=false
    local checkpoint_interval=$7
    local num_unique_keys=$8
    local initial_entries=$9
    local client_p_read=${10}
    local num_logs=${11}
    echo "Client reads $client_p_read%"
    echo "Unique keys $num_unique_keys"
    echo "Initial entries $initial_entries MB"

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
        sudo sed -i s/'=INITIAL_ENTRIES=.*'/=INITIAL_ENTRIES=${initial_entries}/g /etc/systemd/system/bft-smart.service;
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
         ssh -p 22 -o TCPKeepAlive=yes -o ServerAliveInterval=60 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_entry}.emulab.net "sudo service bft-smart start;"
         sleep 5
    done
    sleep 5

    echo "Services reconfigured with paralell $partitioned and checkpoint interval $checkpoint_interval"
    echo "Starting client requests"
    client_cmd="
         sudo truncate -s 0 /srv/logs/*.log;
         tail -f /srv/logs/client.log &;
         cd /srv;
         sudo /usr/bin/java -cp /srv/BFT-SMaRt-parallel-cp-1.0-SNAPSHOT.jar demo.bftmap.BFTMapClientMP $client_client_num_threads 1 $client_num_operations $client_interval $client_max_index $num_unique_keys $client_p_read $client_p_conflict $client_verbose $client_parallel $client_async;
         kill %1;
    "
    ssh -p 22 -o TCPKeepAlive=yes -o ServerAliveInterval=60 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_client}.emulab.net $client_cmd

    echo "Client finished sending requests"
    echo "Sleeping a bit"
    sleep 5s
    echo "Getting remote logs"

    local experiment_dir=experiments/name=sobrecarga/datetime=$datetime/checkpoint=$checkpoint_interval/server_threads=$server_threads/clients=$num_threads/partitioned=${partitioned}/run=$run/read=${client_p_read}/conflict=${client_p_conflict}
    mkdir -p $experiment_dir

    scp ${user_id}@pc${ssh_client}.emulab.net:/srv/logs/client.log $experiment_dir/client.log &
    scp ${user_id}@pc${ssh_client}.emulab.net:/srv/logs/client_latency.log $experiment_dir/client_latency.log &
    for ssh_entry in "${ssh_list[@]}"; do
         scp ${user_id}@pc${ssh_entry}.emulab.net:/srv/logs/throughput.log $experiment_dir/throughput_$ssh_entry.log &
         scp ${user_id}@pc${ssh_entry}.emulab.net:/srv/logs/server.log $experiment_dir/server_$ssh_entry.log &
    done
    wait 
    echo "Logs copied to $experiment_dir folder"
    echo "Experiment has finished"
}


client_num_threads=110
conflito=0
percent_of_read_ops=50 #MB # 200000 ops
num_unique_keys=50
initial_entries=2000 

for checkpoint_interval in 400000 800000 1600000; do    
    # each log = 1 byte
    num_logs=$(($checkpoint_interval / 2))
    
    partitioned=false
    server_threads=4
    
    num_ops=$(($checkpoint_interval + $num_logs)) # total de OPERAÇÕES
    num_ops_by_client=$(($num_ops / $client_num_threads))
    
    echo "Executing $num_ops operações for particionado=$partitioned, numLogs=$num_logs, num_ops_by_client=$num_ops_by_client, server_threads=$server_threads, checkpoint=$checkpoint_interval"
    start_experiment $partitioned $client_num_threads $num_ops_by_client 1 $server_threads $conflito $checkpoint_interval $num_unique_keys $initial_entries $percent_of_read_ops $num_logs; 

    partitioned=true    
    for server_threads in 4 8 16; do        
        echo "Executing $num_ops operações for particionado=$partitioned, numLogs=$num_logs, num_ops_by_client=$num_ops_by_client, server_threads=$server_threads, checkpoint=$checkpoint_interval"

        start_experiment $partitioned $client_num_threads $num_ops_by_client 1 $server_threads $conflito $checkpoint_interval $num_unique_keys $initial_entries $percent_of_read_ops $num_logs;
    done
done
