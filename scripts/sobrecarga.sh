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
    local client_num_threads=$2
    local client_termination_time=$3
    local run=$4
    local server_threads=$5
    local client_interval=$6
    local client_max_index=$server_threads    
    local client_p_conflict=$7
    local client_verbose=false
    local client_parallel=true
    local client_async=false
    local checkpoint_interval=$8
    local num_unique_keys=$9
    local initial_entries=${10}
    local client_p_read=${11}
    local client_timeout=${12}
    echo "Client reads $client_p_read%"
    echo "Unique keys $num_unique_keys"
    echo "Initial entries $initial_entries"

    echo "Ensure client is not running"
    client_cmd="
        sudo killall -9 /usr/bin/java || true;
    "
    ssh -p 22 -o TCPKeepAlive=yes -o ServerAliveInterval=60 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_client}.emulab.net $client_cmd &

    echo "Starting experiments, reconfiguring service's replica"
    reconfigure_cmd="
        sudo service bft-smart stop;
        sudo sed -i s/'=PARTITIONED=.*'/=PARTITIONED=${partitioned}/g /etc/systemd/system/bft-smart.service || true;
        sudo sed -i s/'=PARALLEL=.*'/=PARALLEL=${partitioned}/g /etc/systemd/system/bft-smart.service || true;
        sudo sed -i s/'=THREADS=.*'/=THREADS=${server_threads}/g /etc/systemd/system/bft-smart.service || true;
        sudo sed -i s/'=CHECKPOINT_INTERVAL=.*'/=CHECKPOINT_INTERVAL=${checkpoint_interval}/g /etc/systemd/system/bft-smart.service || true;
        sudo sed -i s/'=INITIAL_ENTRIES=.*'/=INITIAL_ENTRIES=${initial_entries}/g /etc/systemd/system/bft-smart.service || true;
        sudo rm -rf /srv/config/currentView || true;
        sudo rm -rf /disk*/checkpoint*/metadata/* /disk*/checkpoint*/states/* || true;
        sudo rm -rf /srv/logs/*.log || true;
    "

    for ssh_entry in "${ssh_list[@]}"; do
         echo "Reconfiguring $ssh_entry"
         ssh -p 22 -o TCPKeepAlive=yes -o ServerAliveInterval=60 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_entry}.emulab.net "$reconfigure_cmd" &
    done
    wait
    sleep 2
    for ssh_entry in "${ssh_list[@]}"; do
         ssh -p 22 -o TCPKeepAlive=yes -o ServerAliveInterval=60 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_entry}.emulab.net "sudo systemctl daemon-reload && sudo service bft-smart start;"
         sleep 2
    done
    sleep 15

    
    warmup_client_termination_time=5

    echo "Services reconfigured with paralell $partitioned and checkpoint interval $checkpoint_interval"
    #client_cmd="
    #     sudo truncate -s 0 /srv/logs/*.log;
    #     tail -f /srv/logs/client.log &;
    #     cd /srv;
    #     sudo /usr/bin/java -cp /srv/BFT-SMaRt-parallel-cp-1.0-SNAPSHOT.jar demo.bftmap.BFTMapClientMP $client_num_threads 1 $warmup_client_termination_time $client_interval $client_max_index $num_unique_keys $client_p_read $client_p_conflict $client_verbose $client_parallel $client_async $client_timeout;
    #     kill %1;
    #"
    # echo "Warming up"
    # ssh -p 22 -o TCPKeepAlive=yes -o ServerAliveInterval=60 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_client}.emulab.net $client_cmd

    # clean_warmup_logs="
    #     sudo truncate -s 0 /srv/logs/*.log
    # "
    # for ssh_entry in "${ssh_list[@]}"; do
    #      echo "Cleaning logs after warmup $ssh_entry"
    #      ssh -p 22 -o TCPKeepAlive=yes -o ServerAliveInterval=60 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_entry}.emulab.net "$clean_warmup_logs" &
    # done

    client_cmd="
         sudo truncate -s 0 /srv/logs/*.log;
         tail -f /srv/logs/client.log &;
         cd /srv;
         sudo /usr/bin/java -cp /srv/BFT-SMaRt-parallel-cp-1.0-SNAPSHOT.jar demo.bftmap.BFTMapClientMP $client_num_threads 1 $client_termination_time $client_interval $client_max_index $num_unique_keys $client_p_read $client_p_conflict $client_verbose $client_parallel $client_async $client_timeout;
         kill %1;
    "
    echo "Starting client requests"
    ssh -p 22 -o TCPKeepAlive=yes -o ServerAliveInterval=60 -o StrictHostKeyChecking=no ${user_id}@pc${ssh_client}.emulab.net $client_cmd

    echo "Client finished sending requests"
    echo "Sleeping a bit"
    sleep 5
    echo "Getting remote logs"

    local experiment_dir=experiments/name=sobrecarga/datetime=$datetime/checkpoint=$checkpoint_interval/server_threads=$server_threads/clients=$client_num_threads/partitioned=${partitioned}/run=$run/read=${client_p_read}/conflict=${client_p_conflict}
    mkdir -p $experiment_dir

    scp ${user_id}@pc${ssh_client}.emulab.net:/srv/logs/client.log $experiment_dir/client.log &
    scp ${user_id}@pc${ssh_client}.emulab.net:/srv/logs/client_latency.log $experiment_dir/client_latency.log &
    for ssh_entry in "${ssh_list[@]}"; do
         scp ${user_id}@pc${ssh_entry}.emulab.net:/srv/logs/throughput.log $experiment_dir/throughput_$ssh_entry.log &
         scp ${user_id}@pc${ssh_entry}.emulab.net:/srv/logs/server.log $experiment_dir/server_$ssh_entry.log &
         scp ${user_id}@pc${ssh_entry}.emulab.net:/srv/logs/scheduler.log $experiment_dir/scheduler_$ssh_entry.log &
    done
    wait 
    echo "Logs copied to $experiment_dir folder"
    echo "Experiment has finished"
}


conflito=0
percent_of_read_ops=50
num_unique_keys=400
initial_entries=50 # 50 MB
client_termination_time=60 # seconds
client_interval=1000 #millis
client_timeout=50  #Millis
client_num_threads=4
datetime=$(date +%F_%H-%M-%S)

#for checkpoint_interval in 400000 800000; do  
for checkpoint_interval in 240; do  
    #for server_threads in 4 8 16; do
    for server_threads in 4; do
        for partitioned in true ; do
            echo "Executing $num_ops operações for particionado=$partitioned, numLogs=$num_logs, num_ops_by_client=$num_ops_by_client, server_threads=$server_threads, checkpoint=$checkpoint_interval"
            start_experiment $partitioned $client_num_threads $client_termination_time 1 $server_threads $client_interval $conflito $checkpoint_interval $num_unique_keys $initial_entries $percent_of_read_ops $client_timeout;
        done
    done
done
