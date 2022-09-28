/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author eduardo
 */
public class ThroughputStatistics2 implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ThroughputStatistics2.class);
    private final AtomicInteger[] counters;
    private final int[] rawCounters;
    private final int replicaId;

    public ThroughputStatistics2(int numThreads, int replicaId) {
        this.replicaId = replicaId;
        this.counters = new AtomicInteger[numThreads];
        this.rawCounters = new int[numThreads];
        for (int i = 0; i < numThreads; i++) {
            this.counters[i] = new AtomicInteger(0);
            this.rawCounters[i] = 0;
        }
    }

    @Override
    public void run() {
        int ops = 0;
        for (int i = 0; i < counters.length; i++) {
            ops += counters[i].getAndSet(0); 
            //ops += this.rawCounters[i];
            //this.rawCounters[i] = 0;
        }
        logger.info("Replica {} executing {} operations/sec", this.replicaId, ops);
    }

    public void computeStatistics(int threadId, int amount) {
        counters[threadId].incrementAndGet();
        //this.rawCounters[threadId] += amount;
    }
}
