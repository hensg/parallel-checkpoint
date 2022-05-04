package demo.bftmap;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NullReponseException extends Exception {
    public NullReponseException(String msg) {
        super(msg);
    }
}

class Client extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    int id;
    int numberOfOps;
    int interval;
    int numClients;
    int countNumOp = 0;
    int numUniqueKeys;
    int p_pa = 90;
    int p_pb = 5;
    int p_pc = 5;
    boolean verbose;
    PBFTMapMP store;
    int maxIndex;
    int p_conflict;
    int p_read;
    int successOps;
    boolean async;

    public Client(int id, int numberOfOps, int interval, int maxIndex, int numUniqueKeys, boolean verbose, boolean parallel, boolean async,
            int numThreads, int p_read, int p_conflict, ReplyCounterListener replyCounterListener) {
        super("Client " + id);
        this.id = id;
        this.numClients = numThreads;
        this.numberOfOps = numberOfOps;
        this.numUniqueKeys = numUniqueKeys;
        this.p_conflict = p_conflict;
        this.p_read = p_read;
        this.interval = interval;
        this.async = async;
        this.verbose = verbose;
        this.maxIndex = maxIndex;
        this.store = new PBFTMapMP(id, parallel, async, replyCounterListener);
    }

    public void closeProxy() {
        store.closeProxy();
    }

    public void run() {        
        Random r_p = new Random(); // leitura
        Random c_p = new Random();// conflito
        Random q_c = new Random();// conflita com qtas
        Random t1 = new Random();
        Random t2 = new Random();
        Random k1 = new Random();
        Random k2 = new Random();
        Random r_calcResponseTime = new Random();

        boolean shouldCalcResponseTime = false;
        try {
            for (int i = 0; i < numberOfOps && !BFTMapClientMP.stop; i++) {

                long lastSentInstant = 0;
                shouldCalcResponseTime = this.id < 5 && (r_calcResponseTime.nextInt(100) <= 10);
                if (shouldCalcResponseTime)
                    lastSentInstant = System.nanoTime();
                
                BFTMapClientMP.ops[id - BFTMapClientMP.initId]++;

                int r = r_p.nextInt(100); // 0 a 100
                int c = c_p.nextInt(100);

                try {
                    if (r <= p_read && p_read != 0) {// leitura
                    
                        Integer table1 = t1.nextInt(maxIndex);
                        Integer key1 = k1.nextInt(numUniqueKeys);
                        byte[] res = getEntry(store, table1, key1);
                        if (!async && res == null)
                            throw new NullReponseException("Error, got null entries");

                    } else { // escrita
                        if ((c < p_conflict && p_conflict != 0)) {// conflita
                            Integer table1 = t1.nextInt(maxIndex);
                            Integer table2 = t2.nextInt(maxIndex);
                            Integer key1 = k1.nextInt(numUniqueKeys);
                            Integer key2 = k2.nextInt(numUniqueKeys);
                            while (table1.equals(table2)) {
                                table2 = t2.nextInt(numUniqueKeys);
                            }
                            if (table1 > table2) {
                                int aux = table1;
                                table1 = table2;
                                table2 = aux;
                            }
                            byte[] res = putEntries(store, table1, key1, table2, key2);
                            if (!async && res == null) {
                                throw new NullReponseException("Error putting entries, returned null response");
                            }
                        } else {// escrita em 1 particao
                            int table1 = t1.nextInt(maxIndex);
                            int key1 = k1.nextInt(numUniqueKeys);
                            boolean res = insertValue(store, table1, key1);
                            if (!async && !res) {
                                throw new NullReponseException("Failed to insert value, returned a null response");
                            }
                        }
                    }
                    this.successOps += 1;
                    if (shouldCalcResponseTime)
                        logger.info("Latency: {} ns", System.nanoTime() - lastSentInstant);
        
                    if (interval > 0) {
                        try {
                            Thread.sleep(interval);
                        } catch (InterruptedException ex) {
                        }
                    }
                } catch (NullReponseException e) {
                }
            }                      
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int getSuccessOps() {
        return this.successOps;
    }

    private boolean createTable(PBFTMapMP bftMap, Integer nameTable) {
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