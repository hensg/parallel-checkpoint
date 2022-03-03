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
public class ThroughputStatistics implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ThroughputStatistics.class);
    private int[][] counters;
    // private boolean[] restart;
    private final int period = 1000; // millis

    private final int interval = 1000000;
    public ArrayList<Float> values;
    private boolean started = false;
    private AtomicInteger now = new AtomicInteger(0);
    private int replicaId;
    public int zero = 0;
    private int numT = 0;

    // private Timer timer = new Timer();
    public ThroughputStatistics(int numThreads, int replicaId) {
        this.replicaId = replicaId;
        numT = numThreads;
        this.values = new ArrayList();
        counters = new int[numThreads][interval + 1];
        // restart = new boolean[numThreads];
        for (int i = 0; i < numThreads; i++) {
            for (int j = 0; j < interval + 1; j++) {
                counters[i][j] = 0;
            }
        }
    }

    public void computeThroughput(long timeMillis) {

        for (int time = 0; time <= interval; time++) {
            int total = 0;
            for (int i = 0; i < numT; i++) {

                total = total + counters[i][time];

            }

            float tp = (float) (total * 1000 / (float) timeMillis);

            logger.info("Computed throughput for replica {} is {} operations/sec", this.replicaId, tp);
        }
    }

    public void printTP(long timeMillis) {
        int nnow = now.get();
        int total = 0;
        for (int i = 0; i < numT; i++) {
            total = total + counters[i][nnow];
        }

        float tp = (float) (total * 1000 / (float) timeMillis);

        logger.info("Replica {} executing {} operations/sec", this.replicaId, tp);
        if (tp > 0)
            values.add(tp);
    }

    boolean stoped = true;
    int fakenow = 0;

    @Override
    public void run() {
        if (now.get() == interval + 1) {              
            for (int i = 0; i < numT; i++) {
                counters[i][0] = 0;
            }
            now.set(0);     
        } else {
            printTP(period);
            now.incrementAndGet();
        }            
    }

    /*
     * public void start() { if (!started) { started = true; (new
     * Timer()).scheduleAtFixedRate(new TimerTask() { public void run() {
     * computeThroughput(period); } }, period, period); now = 0; } }
     */
    public float getPercentile(int percent) {
        Collections.sort(values);
        int pos = (values.size() - 1) * percent / 100;
        return values.get(pos);
    }

    public void computeStatistics(int threadId, int amount) {
        /*
         * if (restart[threadId]) { counters[threadId] = amount; restart[threadId] =
         * false; } else { counters[threadId] = counters[threadId] + amount; }
         */
        // if (!stoped) {
        counters[threadId][now.get()] = counters[threadId][now.get()] + amount;
        // }

    }

}
