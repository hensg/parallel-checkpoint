package demo.bftmap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import bftsmart.util.Storage;

class Client extends Thread {

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

    public Client(int id, int numberOfOps, int interval, int maxIndex, boolean verbose, boolean parallel, boolean async,
            int numThreads, int p_read, int p_conflict) {
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

    public void closeProxy() {
        store.closeProxy();
    }

    /*
     * private boolean insertValue(int index) {
     * 
     * return store.add(index);
     * 
     * }
     */
    public void run() {

        // logger.info("Warm up...");
        int req = 0;

        Storage st = new Storage(numberOfOps);
        String tableName = "table";

        BFTMapClientMP.logger.info("Executing experiment for {} ops", numberOfOps);
        // if(id==initId){
        // boolean b;
        // try {
        // b = createTable(store,tableName);
        // } catch (Exception ex) {
        // Logger.getLogger(BFTMapClient.class.getName()).log(Level.SEVERE, null, ex);
        // }
        // }
        BFTMapClientMP.op = BFTMapRequestType.PUT;
        int success_ops = 0;
        int error_count = 0;

        for (int i = 0; i < numberOfOps && !BFTMapClientMP.stop; i++, req++) {
            // logger.info("?????????????????");
            if (BFTMapClientMP.op == BFTMapRequestType.PUT) {

                // int index = rand.nextInt(maxIndex);
                int index = maxIndex - 1;
                long last_send_instant = System.nanoTime();
                BFTMapClientMP.ops[id - BFTMapClientMP.initId]++;
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
                        if (res == null)
                            throw new RuntimeException("Error, got null entries");
                    } else { // escrita
                        if ((c < p_conflict && p_conflict != 0)) {// conflita
                            BFTMapClientMP.var++;
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
                            // logger.info("INCORRECT TABLE");
                            // break;
                            // }
                            if (table1 > table2) {
                                int aux = table1;
                                table1 = table2;
                                table2 = aux;
                            }
                            long last = System.nanoTime();
                            byte[] res = putEntries(store, table1, key1, table2, key2);
                            if (res == null) {
                                throw new RuntimeException("Error putting entries, returned null response");
                            }
                        } else {// escrita em 1 particao
                            int table1 = t1.nextInt(maxIndex);
                            int key1 = k1.nextInt(maxIndex);
                            // logger.info("here?");
                            long last = System.nanoTime();
                            boolean res = insertValue(store, table1, key1);
                            if (!res) {
                                throw new RuntimeException("Failed to insert value, returned a null response");
                            }
                        }
                    }
                    // logger.info("sent op");
                    // logger.info("inserting into table"+table);
                    // result = insertValue(store,table,2);
                    success_ops += 1;
                } catch (Exception ex) {
                    BFTMapClientMP.logger.error("Sending data operation failed, message: {}", ex.getMessage());
                }
                // logger.info(st.store(System.nanoTime() - last_send_instant));
                st.store(System.nanoTime() - last_send_instant);
            }

            if (interval > 0) {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException ex) {
                }
            }

            // if (verbose && (req % 1000 == 0)) {
            // BFTMapClientMP.logger.info("{} operations sent by {}!", success_ops,
            // this.id);
            // }
        }
        BFTMapClientMP.logger.info("Total of operations sent successfully by client {} = {}", id, success_ops);
        BFTMapClientMP.logger.info("Total conflict of client {} = {}", id, BFTMapClientMP.var);

        BFTMapClientMP.logger.info(this.id + " // Average time for " + numberOfOps + " executions (-10%) = " + st.getAverage(true) / 1000 + " us ");
        BFTMapClientMP.logger.info(this.id + " // Standard deviation for " + numberOfOps + " executions (-10%) = " + st.getDP(true) / 1000 + " us ");
        BFTMapClientMP.logger.info(this.id + " // Average time for " + numberOfOps + " executions (all samples) = " + st.getAverage(false) / 1000 + " us ");
        BFTMapClientMP.logger.info(this.id + " // Standard deviation for " + numberOfOps + " executions (all samples) = " + st.getDP(false) / 1000 + " us ");
        BFTMapClientMP.logger.info(this.id + " // 90th percentile for " + numberOfOps + " executions = " + st.getPercentile(90) / 1000 + " us ");
        BFTMapClientMP.logger.info(this.id + " // 95th percentile for " + numberOfOps + " executions = " + st.getPercentile(95) / 1000 + " us ");
        BFTMapClientMP.logger.info(this.id + " // 99th percentile for " + numberOfOps + " executions = " + st.getPercentile(99) / 1000 + " us ");
    }

    private boolean createTable(PBFTMapMP bftMap, Integer nameTable) throws Exception {
        boolean tableExists;

        tableExists = bftMap.containsKey(nameTable);
        BFTMapClientMP.logger.info("tableExists: {}", tableExists);
        if (tableExists == false)
            bftMap.put(nameTable, new TreeMap<Integer, byte[]>());
        BFTMapClientMP.logger.info("Created the table. Maybe");

        return tableExists;
    }

    private boolean insertValue(PBFTMapMP bftMap, Integer nameTable, int index) throws Exception {
        Integer key = index;
        Random rand = new Random();
        int obj = rand.nextInt();
        byte[] valueBytes = ByteBuffer.allocate(1024).array();
        // Random rand = new Random();
        // byte[] valueBytes = new byte[VALUE_SIZE];
        // rand.nextBytes(valueBytes);
        byte[] resultBytes = bftMap.putEntry(nameTable, key, valueBytes);
        // logger.info("resultBytes" + resultBytes);
        if (resultBytes == null)
            return false;
        return true;

    }

    private byte[] putEntries(PBFTMapMP bftMap, Integer nameTable1, Integer key1, Integer nameTable2, Integer key2)
            throws Exception {
        Integer k1 = key1;
        Integer table1 = nameTable1;
        Integer table2 = nameTable2;
        Integer k2 = key2;
        Random rand = new Random();
        int obj = rand.nextInt();
        byte[] valueBytes = ByteBuffer.allocate(1024).array();
        // logger.info("Here?????");
        // Random rand = new Random();
        // byte[] valueBytes = new byte[VALUE_SIZE];
        // rand.nextBytes(valueBytes);
        byte[] resultBytes = bftMap.putEntries(nameTable1, key1, nameTable2, key2, valueBytes);
        // logger.info("resultBytes" + resultBytes);
        if (resultBytes == null)
            return null;
        return resultBytes;

    }

    private int getSizeTable(PBFTMapMP bftMap, Integer tableName) throws Exception {
        int res = bftMap.size1(tableName);
        if (res == -1)
            throw new Exception();
        return res;
    }

    private byte[] getEntry(PBFTMapMP bftMap, Integer tableName, Integer key) {
        byte[] res = bftMap.getEntry(tableName, key);
        if (res == null)
            return null;
        return res;
    }

    private byte[] getEntries(PBFTMapMP bftMap, Integer tableName1, Integer key1, Integer tablename2, Integer key2) {
        byte[] res = bftMap.getEntries(tableName1, key1, tablename2, key2);
        if (res == null)
            return null;
        return res;
    }
}