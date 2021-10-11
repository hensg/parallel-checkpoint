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

//import bftsmart.tom.parallelism.ParallelMapping;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import demo.bftmap.Client;

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
    public static void main(String[] args) throws IOException {
        if (args.length < 7) {
            logger.info(
                    "Usage: ... BFTMapClientMP <num. threads> <process id> <number of operations> <interval> <maxIndex> <p_read %> <p_conflict %> <verbose?> <parallel?> <async?>");
            System.exit(-1);
        }

        int numThreads = Integer.parseInt(args[0]);
        initId = Integer.parseInt(args[1]);

        int numberOfOps = Integer.parseInt(args[2]);
        // int requestSize = Integer.parseInt(args[3]);
        int interval = Integer.parseInt(args[3]);
        int max = Integer.parseInt(args[4]);
        int p_conflict = Integer.parseInt(args[6]);
        int p_read = Integer.parseInt(args[5]);
        boolean verbose = Boolean.parseBoolean(args[7]);
        boolean parallel = Boolean.parseBoolean(args[8]);
        boolean async = Boolean.parseBoolean(args[9]);
        logger.info("P_CONFLICT ====== {}", p_conflict);
        Client[] c = new Client[numThreads];
        ops = new int[numThreads];
        for (int k = 0; k < ops.length; k++) {
            ops[k] = 0;
        }
        for (int i = 0; i < numThreads; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                logger.error("", ex.getCause());
            }

            logger.info("Launching client {}", (initId + i));
            c[i] = new Client(initId + i, numberOfOps, interval, max, verbose, parallel, async, numThreads, p_read,
                    p_conflict);
            // c[i].start();
        }

        // try {
        // Thread.sleep(30000);
        // } catch (InterruptedException ex) {
        // Logger.getLogger(ListClient.class.getName()).log(Level.SEVERE, null, ex);
        // }

        for (int i = 0; i < numThreads; i++) {
            c[i].start();
        }

        // @author Henrique - commented useless threads
        // (new Timer()).scheduleAtFixedRate(new TimerTask() {
        // public void run() {
        // // change();
        // }
        // }, 60000, 60000); // a cada 1 minuto

        // (new Timer()).schedule(new TimerTask() {
        // public void run() {
        // stop();
        // }
        // }, 5 * 60000); // depois de 5 minutos

        for (int i = 0; i < numThreads; i++) {
            try {
                c[i].join(1000 * 60);
                // @author Henrique - add close proxy calll
                c[i].closeProxy();
                logger.info("Client thread {} completed", c[i].id);
            } catch (InterruptedException ex) {
                logger.error("Waiting thread finish... interrupted", ex);
            }
        }
        logger.info("Finished all client threads execution...");
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
