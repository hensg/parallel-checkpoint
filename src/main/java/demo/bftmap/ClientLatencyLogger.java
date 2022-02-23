package demo.bftmap;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClientLatencyLogger implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(ClientLatencyLogger.class);
    private long maxLatency = 0;

    public ClientLatencyLogger() {
        logger.info("Creating latency logger");
    }

    public void insert(long latencyNano) {
        if (latencyNano > maxLatency)
            maxLatency = latencyNano;
    }

    private void reset() {
        maxLatency = 0;
    }

    @Override
    public void run() {
        logger.info("Latency: {} ns", maxLatency);
        reset();
    }
}
