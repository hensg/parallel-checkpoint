package demo.bftmap;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client syncronous implementation used to calculate the latency while
 * asynchronous clients are dispatching requests.
 * 
 * @henrique.goulart
 **/
class ClientLatency extends Client {

  private final Logger logger = LoggerFactory.getLogger(ClientLatency.class);
  private final static int ONE_MILLION = 1_000_000;

  public ClientLatency(int id, int maxIndex, int numUniqueKeys, boolean verbose, boolean parallel, boolean async,
      int numThreads, int p_read, int p_conflict, int interval, int timeout) {
    super(id, maxIndex, numUniqueKeys, verbose, parallel, async, numThreads, p_read, p_conflict, interval, timeout);
  }

  ExecutorService pool = Executors.newScheduledThreadPool(1);

  @Override
  public void run() {
    final long lastSentInstant = System.nanoTime();
    final Future<?> fut = pool.submit(new Runnable() {
      public void run() {
        try {
          insertValue(store, roundTable, roundKey);
        } catch (Exception e) {
          logger.error("Failed to insert value", e);
          System.exit(0);
        }
      }
    });

    try {
      fut.get(timeout, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
    }

    final long latency = System.nanoTime() - lastSentInstant;
    logger.info("Count {}, Latency {}millis", this.countNumOp, latency / ONE_MILLION);

    roundTable = random.nextInt(this.maxIndex);
    roundKey = random.nextInt(this.numUniqueKeys);
    // this.roundTable = (roundTable + 1) % this.maxIndex;
    // this.roundKey = (roundKey + 1) % this.numUniqueKeys;
    this.countNumOp += 1;
  }

}