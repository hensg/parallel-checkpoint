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
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

//import bftsmart.tom.ServiceProxy;
//import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.util.Storage;

/**
 * Example client that updates a BFT replicated service (a counter).
 *
 */
public class BFTMapClientMP {
    private static int var = 0;
    private static int VALUE_SIZE = 1024;
    public static int initId = 0;
    public static int[] ops;
    public static int op = BFTMapRequestType.PUT;
    public static boolean stop = false;
    public static boolean created = false;

    @SuppressWarnings("static-access")
    public static void main(String[] args) throws IOException {
        if (args.length < 7) {
            System.out.println(
                    "Usage: ... ListClient <num. threads> <process id> <number of operations> <interval> <maxIndex> <p_read %> <p_conflict %> <verbose?> <parallel?> <async?>");
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
        System.out.println("P_CONFLICT ====== " + p_conflict);
        Client[] c = new Client[numThreads];
        ops = new int[numThreads];
        for (int k = 0; k < ops.length; k++) {
            ops[k] = 0;
        }
        for (int i = 0; i < numThreads; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                Logger.getLogger(BFTMapClientMP.class.getName()).log(Level.SEVERE, null, ex);
            }

            System.out.println("Launching client " + (initId + i));
            c[i] = new BFTMapClientMP.Client(initId + i, numberOfOps, interval, max, verbose, parallel, async,
                    numThreads, p_read, p_conflict);
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

        (new Timer()).scheduleAtFixedRate(new TimerTask() {
            public void run() {
                // change();
            }
        }, 60000, 60000); // a cada 1 minuto

        (new Timer()).schedule(new TimerTask() {
            public void run() {
                stop();
            }
        }, 5 * 60000); // depois de 5 minutos

        for (int i = 0; i < numThreads; i++) {

            try {
                c[i].join();
            } catch (InterruptedException ex) {
                ex.printStackTrace(System.err);
            }
        }

        // System.exit(0);
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

    static class Client extends Thread {

        int id;
        int numberOfOps;
        int interval;
        int numClients;
        int countNumOp = 0;
        int p_pa = 90;
        int p_pb = 5;
        int p_pc = 5;
        boolean verbose;
        // boolean dos;
        // ServiceProxy proxy;
        // byte[] request;
        // BFTMap bftMap = new BFTMap(id);
        // BFTList<Integer> store;
        PBFTMapMP store;
        int maxIndex;
        int p_conflict;
        int p_read;
        // int percent;

        public Client(int id, int numberOfOps, int interval, int maxIndex, boolean verbose, boolean parallel,
                boolean async, int numThreads, int p_read, int p_conflict) {
            super("Client " + id);
            this.id = id;
            this.numClients = numThreads;
            this.numberOfOps = numberOfOps;
            // this.percent = percent;
            this.p_conflict = p_conflict;
            this.p_read = p_read;
            this.interval = interval;

            this.verbose = verbose;
            // this.proxy = new ServiceProxy(id);
            // this.request = new byte[this.requestSize];
            this.maxIndex = maxIndex;

            store = new PBFTMapMP(id, parallel, async);
            // this.dos = dos;
        }

        /*
         * private boolean insertValue(int index) {
         * 
         * return store.add(index);
         * 
         * }
         */
        public void run() {

            // System.out.println("Warm up...");
            int req = 0;

            Storage st = new Storage(numberOfOps);
            String tableName = "table";

            System.out.println("Executing experiment for " + numberOfOps + " ops");
            // if(id==initId){
            // boolean b;
            // try {
            // b = createTable(store,tableName);
            // } catch (Exception ex) {
            // Logger.getLogger(BFTMapClient.class.getName()).log(Level.SEVERE, null, ex);
            // }
            // }
            op = BFTMapRequestType.PUT;
            for (int i = 0; i < numberOfOps && !stop; i++, req++) {

                // System.out.println("?????????????????");
                if (op == BFTMapRequestType.PUT) {

                    // int index = rand.nextInt(maxIndex);
                    int index = maxIndex - 1;
                    long last_send_instant = System.nanoTime();
                    ops[id - initId]++;
                    Random insert = new Random();
                    // int ent = insert.nextInt(2); //to numtables
                    Random rand = new Random();
                    // 512mb de estado
                    // int next = rand.nextInt(32);
                    // int val = rand.nextInt(16); //16 registros de 1MB

                    // 1GB de estado
                    int table = rand.nextInt(maxIndex); // tabelas
                    // int key = rand.nextInt(262144); //chaves por tabela
                    boolean result;
                    try {
                        Random r_p = new Random(); // leitura
                        Random c_p = new Random();// conflito
                        Random q_c = new Random();// conflita com qtas
                        Random t1 = new Random();
                        Random t2 = new Random();
                        Random t3 = new Random();
                        Random t4 = new Random();
                        Random k1 = new Random();
                        Random k2 = new Random();
                        Random k3 = new Random();
                        Random k4 = new Random();
                        int r = r_p.nextInt(100); // 0 a 100
                        int c = c_p.nextInt(100);
                        if (r < p_read && p_read != 0) {// leitura
                            Integer table1 = t1.nextInt(maxIndex);
                            Integer key1 = k1.nextInt(maxIndex);
                            long last = System.nanoTime();
                            byte[] res = getEntry(store, table1, key1);
                            if (id == 4)
                                System.out.println(System.nanoTime() + " " + (System.nanoTime() - last));
                        } else { // escrita
                            if ((c < p_conflict && p_conflict != 0)) {// conflita
                                var++;
                                Integer q = ThreadLocalRandom.current().nextInt(2, maxIndex); // de 2 a n_particoes
                                Integer table1 = t1.nextInt(maxIndex);
                                Integer table2 = t2.nextInt(maxIndex);
                                Integer key1 = k1.nextInt(maxIndex);
                                Integer key2 = k2.nextInt(maxIndex);
                                while (table1 == table2) {
                                    table2 = t2.nextInt(maxIndex);
                                }
                                // switch(table1){
                                // case 1:
                                // table2=2;
                                // break;
                                // case 2:
                                // table2=1;
                                // break;
                                // case 3:
                                // table2=4;
                                // break;
                                // case 4:
                                // table2=3;
                                // break;
                                // default:
                                // System.out.println("INCORRECT TABLE");
                                // break;
                                // }
                                if (table1 > table2) {
                                    int aux = table1;
                                    table1 = table2;
                                    table2 = aux;
                                }
                                long last = System.nanoTime();
                                byte[] res = putEntries(store, table1, key1, table2, key2);
                                if (id == 4)
                                    System.out.println(System.nanoTime() + " " + (System.nanoTime() - last));
                            } else {// escrita em 1 particao
                                int table1 = t1.nextInt(maxIndex);
                                int key1 = k1.nextInt(maxIndex);
                                // System.out.println("here?");
                                long last = System.nanoTime();
                                boolean res = insertValue(store, table1, key1);
                                if (id == 4)
                                    System.out.println(System.nanoTime() + " " + (System.nanoTime() - last));
                            }
                        }
                        // System.out.println("sent op");
                        // System.out.println("inserting into table"+table);
                        // result = insertValue(store,table,2);
                    } catch (Exception ex) {
                        Logger.getLogger(BFTMapClientMP.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    // System.out.println(st.store(System.nanoTime() - last_send_instant));
                    st.store(System.nanoTime() - last_send_instant);
                }

                if (interval > 0 && i % 50 == 100) {
                    try {
                        Thread.sleep(interval);
                    } catch (InterruptedException ex) {
                    }
                }

                if (verbose && (req % 1000 == 0)) {
                    System.out.println(this.id + " // " + req + " operations sent!");
                }

            }

            System.out.println("total conflict of client " + id + " = " + var);
            try {
                sleep(4000);
            } catch (InterruptedException ex) {
                Logger.getLogger(BFTMapClientMP.class.getName()).log(Level.SEVERE, null, ex);
            }
            // store.printValues();
            if (id == initId) {
                // System.out.println(this.id + " // Average time for " + numberOfOps + "
                // executions (-10%) = " + st.getAverage(true) / 1000 + " us ");
                // System.out.println(st.getAverage(true) / 1000);
                // System.out.println(this.id + " // Standard desviation for " + numberOfOps + "
                // executions (-10%) = " + st.getDP(true) / 1000 + " us ");
                // System.out.println(this.id + " // Average time for " + numberOfOps / 2 + "
                // executions (all samples) = " + st.getAverage(false) / 1000 + " us ");
                // System.out.println(this.id + " // Standard desviation for " + numberOfOps / 2
                // + " executions (all samples) = " + st.getDP(false) / 1000 + " us ");
                // System.out.println(this.id + " // 90th percentile for " + numberOfOps + "
                // executions = " + store.getPercentile(90) / 1000 + " us ");
                // System.out.println(this.id + " // 95th percentile for " + numberOfOps + "
                // executions = " + st.getPercentile(95) / 1000 + " us ");
                // System.out.println(this.id + " // 99th percentile for " + numberOfOps + "
                // executions = " + st.getPercentile(99) / 1000 + " us ");
            }

        }

        private static boolean createTable(PBFTMapMP bftMap, Integer nameTable) throws Exception {
            boolean tableExists;

            tableExists = bftMap.containsKey(nameTable);
            System.out.println("tableExists:" + tableExists);
            if (tableExists == false)
                bftMap.put(nameTable, new TreeMap<Integer, byte[]>());
            System.out.println("Created the table. Maybe");

            return tableExists;
        }

        private static boolean insertValue(PBFTMapMP bftMap, Integer nameTable, int index) throws Exception {
            Integer key = index;
            Random rand = new Random();
            int obj = rand.nextInt();
            byte[] valueBytes = ByteBuffer.allocate(1024).array();
            // Random rand = new Random();
            // byte[] valueBytes = new byte[VALUE_SIZE];
            // rand.nextBytes(valueBytes);
            byte[] resultBytes = bftMap.putEntry(nameTable, key, valueBytes);
            // System.out.println("resultBytes" + resultBytes);
            if (resultBytes == null)
                return false;
            return true;

        }

        private static byte[] putEntries(PBFTMapMP bftMap, Integer nameTable1, Integer key1, Integer nameTable2,
                Integer key2) throws Exception {
            Integer k1 = key1;
            Integer table1 = nameTable1;
            Integer table2 = nameTable2;
            Integer k2 = key2;
            Random rand = new Random();
            int obj = rand.nextInt();
            byte[] valueBytes = ByteBuffer.allocate(1024).array();
            // System.out.println("Here?????");
            // Random rand = new Random();
            // byte[] valueBytes = new byte[VALUE_SIZE];
            // rand.nextBytes(valueBytes);
            byte[] resultBytes = bftMap.putEntries(nameTable1, key1, nameTable2, key2, valueBytes);
            // System.out.println("resultBytes" + resultBytes);
            if (resultBytes == null)
                return null;
            return resultBytes;

        }

        private static int getSizeTable(PBFTMapMP bftMap, Integer tableName) throws Exception {
            int res = bftMap.size1(tableName);
            if (res == -1)
                throw new Exception();
            return res;
        }

        private static byte[] getEntry(PBFTMapMP bftMap, Integer tableName, Integer key) {
            byte[] res = bftMap.getEntry(tableName, key);
            if (res == null)
                return null;
            return res;
        }

        private static byte[] getEntries(PBFTMapMP bftMap, Integer tableName1, Integer key1, Integer tablename2,
                Integer key2) {
            byte[] res = bftMap.getEntries(tableName1, key1, tablename2, key2);
            if (res == null)
                return null;
            return res;
        }
    }
}
