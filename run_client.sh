 CLIENT_NUM_THREADS=1
 CLIENT_TERMINATION_TIME=5000
 CLIENT_INTERVAL=1000
 CLIENT_MAX_INDEX=2
 NUM_UNIQUE_KEYS=4
 CLIENT_P_READ=0
 CLIENT_P_CONFLICT=0
 CLIENT_VERBOSE=false
 CLIENT_PARALLEL=true
 CLIENT_ASYNC=false
 CLIENT_TIMEOUT=100


/usr/bin/java \
  -cp target/BFT-SMaRt-parallel-cp-1.0-SNAPSHOT.jar \
  demo.bftmap.BFTMapClientMP \
  $CLIENT_NUM_THREADS 1 \
  $CLIENT_TERMINATION_TIME \
  $CLIENT_INTERVAL \
  $CLIENT_MAX_INDEX \
  $NUM_UNIQUE_KEYS \
  $CLIENT_P_READ \
  $CLIENT_P_CONFLICT \
  $CLIENT_VERBOSE \
  $CLIENT_PARALLEL \
  $CLIENT_ASYNC \
  $CLIENT_TIMEOUT;

