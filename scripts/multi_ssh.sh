#!/bin/bash

ssh_list=${1}
IFS=',' read -ra ssh_list <<< "$ssh_list"

tmux new-window "ssh -p 22 hensg@pc${ssh_list[0]}.emulab.net"

unset ssh_list[0];
for ssh_entry in "${ssh_list[@]}"
do
    tmux split-window -h "ssh -p 22 -o StrictHostKeyChecking=no hensg@pc${ssh_entry}.emulab.net"
    tmux select-layout tiled > /dev/null
done

tmux select-pane -t 0
tmux set-window-option synchronize-panes on > /dev/null
