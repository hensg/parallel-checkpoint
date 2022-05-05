package demo.bftmap;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClientLatencyLogger implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(ClientLatencyLogger.class);
    private final AtomicInteger latencyIndex = new AtomicInteger(0);
    private final int MAX_INDEX = 1000000;
    private final long[] latencies = new long[MAX_INDEX+1];

    public ClientLatencyLogger() {
        logger.info("Creating latency logger");       
    }

    public void addLatency(long latencyNS) {
        int index = latencyIndex.getAndAdd(1);        
        if (index < MAX_INDEX) {
            latencies[index] = latencyNS;
        }
    }
    
    @Override
    public void run() {
        long avgLatency = 0;
        int lastLatencyIndex = latencyIndex.get() - 1;
        if (lastLatencyIndex > MAX_INDEX)
            lastLatencyIndex = MAX_INDEX;
        for (int i = 0; i < lastLatencyIndex; i++) {
            avgLatency += latencies[i];
        }
        avgLatency /= lastLatencyIndex;
        logger.info("Latency: {} ns", avgLatency);
        latencyIndex.set(0);
    }
    
    public void logLatency(long latency) {
        logger.info("Latency: {} ns", latency);
    }
}
