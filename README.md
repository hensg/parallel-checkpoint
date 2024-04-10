# BFT Smart
Byzantine Fault-Tolerant (BFT) State Machine Replication (SMaRt) v1.2


# Parallel Checkpoint

Build the project:
```
$ mvn clean package
```

Run server:
```
$ ./run_replica.sh 0
$ ./run_replica.sh 1
$ ./run_replica.sh 2
```

Run client:
```
$ ./run_client.sh
```
