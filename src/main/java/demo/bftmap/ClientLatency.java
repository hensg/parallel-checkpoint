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
                    if (random.nextInt(100) < p_read) {
                        getEntry(store, roundTable, roundKey);
                    } else {
                        if (random.nextInt(100) < p_conflict) {
                            int roundKey2 = random.nextInt(maxIndex);
                            int roundTable2 = random.nextInt(numUniqueKeys);

                            while (roundKey2 == roundKey)
                                roundKey2 = random.nextInt(maxIndex);
                            while (roundTable2 == roundTable)
                                roundTable2 = random.nextInt(numUniqueKeys);

                            if (roundTable > roundTable2) {
                                // do not remove it, its need for hashcode that is based only in asc order of
                                // ids
                                int aux = roundTable;
                                roundTable = roundTable2;
                                roundTable2 = aux;
                            }
                            putEntries(store, roundTable, roundKey, roundTable2, roundKey2);
                        } else {
                            insertValue(store, roundTable, roundKey);
                        }
                    }
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
        this.countNumOp += 1;
    }

}
