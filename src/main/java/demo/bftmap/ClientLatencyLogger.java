package demo.bftmap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.util.Storage;

class ClientLatencyLogger implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(ClientLatencyLogger.class);
    private Storage st;

    public ClientLatencyLogger(Storage st) {
        logger.info("Creating latency logger");
        this.st = st;
    }

    
    @Override
    public void run() {
        logger.info("Latency: {} ns", st.getAverage(false));  
    }
}
