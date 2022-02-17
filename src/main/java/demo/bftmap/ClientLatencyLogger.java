package demo.bftmap;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClientLatencyLogger implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(ClientLatencyLogger.class);
    private final List<Long> latencies = new ArrayList<Long>();
    private static final int PERCENTILE = 95;

    public ClientLatencyLogger() {
        logger.info("Creating latency logger");
    }

    public void insert(long latencyNano) {
        latencies.add(latencyNano);
    }

    private void reset() {
        this.latencies.clear();
    }

    @Override
    public void run() {
        try {
            int index = (int) Math.ceil(latencies.size() * PERCENTILE / 100);
            if (index <= 0)
                return;
            logger.info("Latency(95pct): {} ns", latencies.get(index-1));
            reset();
        } catch (Exception e) {
            logger.error("Error calculating latency", e);
        }
    }
}