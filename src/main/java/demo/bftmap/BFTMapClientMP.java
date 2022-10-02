/**
 * Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and
 * the authors indicated in the @author tags
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package demo.bftmap;

// import bftsmart.tom.parallelism.ParallelMapping;
import bftsmart.util.Storage;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example client that updates a BFT replicated service (a counter).
 *
 */
public class BFTMapClientMP {

    static final Logger logger = LoggerFactory.getLogger(BFTMapClientMP.class);

    static int var = 0;
    private static int VALUE_SIZE = 1024;
    public static int initId = 0;
    public static int[] ops;
    public static int op = BFTMapRequestType.PUT;
    public static boolean stop = false;
    public static boolean created = false;

    @SuppressWarnings("static-access")
    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            logger.info(
                    "Usage: ... BFTMapClientMP <num. threads> <process id> <time to run> <interval> <maxIndex> <numUniqueKeys> <p_read %> <p_conflict %> <verbose?> <parallel?> <async?>");
            System.exit(-1);
        }

        int numThreads = Integer.parseInt(args[0]);
        initId = Integer.parseInt(args[1]);

        int terminationTime = Integer.parseInt(args[2]);
        // int requestSize = Integer.parseInt(args[3]);
        int interval = Integer.parseInt(args[3]);
        int max = Integer.parseInt(args[4]);
        int numUniqueKeys = Integer.parseInt(args[5]);
        int p_conflict = Integer.parseInt(args[7]);
        int p_read = Integer.parseInt(args[6]);
        boolean verbose = Boolean.parseBoolean(args[8]);
        boolean parallel = Boolean.parseBoolean(args[9]);
        boolean async = Boolean.parseBoolean(args[10]);
        int timeout = Integer.parseInt(args[11]);
        logger.info("P_CONFLICT ====== {}", p_conflict);

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(numThreads);

        Client[] clients = new Client[numThreads];
        for (int i = 0; i < numThreads; i++) {
            clients[i] = new Client(i, max, numUniqueKeys, verbose, parallel, async, numThreads, p_read, p_conflict,
                    interval,
                    timeout);

            executorService.scheduleAtFixedRate(clients[i], 100 + i * 10, interval, TimeUnit.MILLISECONDS);
        }

        //ScheduledExecutorService latencyScheduler = Executors.newSingleThreadScheduledExecutor();
        //ClientLatency clientLatency = new ClientLatency(999999, max, numUniqueKeys, verbose, parallel, async,
        //        numThreads, p_read, p_conflict, interval, timeout);
        //latencyScheduler.scheduleAtFixedRate(clientLatency, 5, 1, TimeUnit.SECONDS);

        executorService.awaitTermination(terminationTime, TimeUnit.SECONDS);
        logger.info("Finished all client threads execution...");
        System.exit(0);
    }

    public static void stop() {
        stop = true;
    }

    public static void change() {
        if (op == BFTMapRequestType.CHECK) {
            op = BFTMapRequestType.PUT;
        } else {
            op = BFTMapRequestType.CHECK;
        }
    }
}
