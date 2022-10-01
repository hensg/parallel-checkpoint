NODE_ID=$1
INTERVAL=0
THREADS=2
INITIAL_ENTRIES=10
CHECKPOINT_INTERVAL=6
PARALLEL=true
NUM_DISKS=1
MEMORY=4

/usr/bin/java \
  -XX:+AlwaysPreTouch \
  -XX:+UseStringDeduplication \
  -XX:+UseG1GC \
  -Xms${MEMORY}g -Xmx${MEMORY}g \
  -cp target/BFT-SMaRt-parallel-cp-1.0-SNAPSHOT.jar \
  demo.bftmap.BFTMapServerMP \
  $NODE_ID \
  $INTERVAL \
  $THREADS \
  $INITIAL_ENTRIES \
  $CHECKPOINT_INTERVAL \
  $PARALLEL \
  $NUM_DISKS
