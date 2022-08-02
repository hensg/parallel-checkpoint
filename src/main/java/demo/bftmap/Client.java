package demo.bftmap;

import bftsmart.demo.bftmap.BFTMap;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Client implements Runnable {

    protected static final Logger logger = LoggerFactory.getLogger(Client.class);
    int id;
    int numberOfOps;
    int interval;
    int numClients;
    int countNumOp;
    int numUniqueKeys;
    int p_pa = 90;
    int p_pb = 5;
    int p_pc = 5;
    boolean verbose;
    final PBFTMapMP store;
    int maxIndex;
    int p_conflict;
    int p_read;
    int successOps;
    boolean async;
    final Random random = new Random();
    int roundKey;
    int roundTable;
    int timeout;

    public Client(int id, int maxIndex, int numUniqueKeys, boolean verbose, boolean parallel, boolean async,
                  int numThreads, int p_read, int p_conflict, int interval, int timeout) {
        this.id = id;
        this.numClients = numThreads;
        this.numUniqueKeys = numUniqueKeys;
        this.p_conflict = p_conflict;
        this.p_read = p_read;
        this.async = async;
        this.verbose = verbose;
        this.maxIndex = maxIndex;
        this.roundKey = 0;
        this.roundTable = 0;
        this.countNumOp = 0;
        this.interval = interval;
        this.timeout = timeout;
        this.store = new PBFTMapMP(id, parallel, async, null);
        logger.info("Started new client {}", id);
    }

    public void closeProxy() { store.closeProxy(); }

    private final static float CALC_LATENCY_THRESHOLD = 0.0005f;
    private final static int ONE_MILLION = 1_000_000;
    ExecutorService pool = Executors.newScheduledThreadPool(1);

    public void run() {
        final float randomValue = random.nextFloat();

        final Future<?> fut = pool.submit(new Runnable() {
            public void run() {
                try {
                    insertValue(store, roundTable, roundKey);
                } catch (Exception e) {
                    logger.error("Failed to insert value", e);
                    System.exit(0);
                }
            }
        });

        try {
            fut.get(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
        }

        roundTable = random.nextInt(this.maxIndex);
        roundKey = random.nextInt(this.numUniqueKeys);
        //this.roundTable = (roundTable + 1) % this.maxIndex;
        //this.roundKey = (roundKey + 1) % this.numUniqueKeys;
        this.countNumOp += 1;
    }

    public int getSuccessOps() { return this.successOps; }

    protected boolean insertValue(PBFTMapMP bftMap, Integer nameTable, int index) throws Exception {
        Integer key = index;
        byte[] valueBytes = ByteBuffer.allocate(1024).array();
        byte[] resultBytes = bftMap.putEntry(nameTable, key, valueBytes);
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
