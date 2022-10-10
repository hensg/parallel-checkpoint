package demo.bftmap;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Client implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Client.class);
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
    Blacklist blacklist;

    public Client(int id, int maxIndex, int numUniqueKeys, boolean verbose, boolean parallel, boolean async,
                  int numThreads, int p_read, int p_conflict, int interval, int timeout, Blacklist blacklist) {
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
        this.blacklist = blacklist;
        logger.info("Started new client {} - maxIndex {}, numUniqueKeys {}", id, this.maxIndex, this.numUniqueKeys);
    }

    public void closeProxy() {
        store.closeProxy();
    }

    ExecutorService pool = Executors.newScheduledThreadPool(1);


    public void run() {
        roundKey = random.nextInt(this.numUniqueKeys);
        roundTable = random.nextInt(this.maxIndex);
        if (blacklist.contains(roundTable))
            return;

        try {
            if (random.nextInt(100) < p_read) {
                getEntry(store, roundTable, roundKey);
            } else {
                if (random.nextInt(100) < p_conflict) {
                    int roundKey2 = random.nextInt(maxIndex);
                    int roundTable2 = random.nextInt(numUniqueKeys);

                    while (roundKey2 == roundKey)
                        roundKey2 = random.nextInt(maxIndex);
                    while (roundTable2 == roundTable)
                        roundTable2 = random.nextInt(numUniqueKeys);

                    if (roundTable > roundTable2) {
                        // do not remove it, its need for hashcode that is based only in asc order of
                        // ids
                        int aux = roundTable;
                        roundTable = roundTable2;
                        roundTable2 = aux;
                    }
                    putEntries(store, roundTable, roundKey, roundTable2, roundKey2);
                } else {
                    insertValue(store, roundTable, roundKey);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to insert value into table: {}, key: {}", roundTable, roundKey, e);
            System.exit(0);
        }

        this.countNumOp += 1;
    }

    public int getSuccessOps() {
        return this.successOps;
    }

    protected boolean insertValue(PBFTMapMP bftMap, Integer nameTable, int index) throws Exception {
        Integer key = index;
        byte[] valueBytes = ByteBuffer.allocate(1024).array();
        byte[] resultBytes = bftMap.putEntry(nameTable, key, valueBytes);
        if (resultBytes == null)
            return false;
        return true;
    }

    protected byte[] putEntries(PBFTMapMP bftMap, Integer nameTable1, Integer key1, Integer nameTable2, Integer key2)
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

    protected int getSizeTable(PBFTMapMP bftMap, Integer tableName) throws Exception {
        int res = bftMap.size1(tableName);
        if (res == -1)
            throw new Exception();
        return res;
    }

    protected byte[] getEntry(PBFTMapMP bftMap, Integer tableName, Integer key) {
        byte[] res = bftMap.getEntry(tableName, key);
        if (res == null)
            return null;
        return res;
    }

    protected byte[] getEntries(PBFTMapMP bftMap, Integer tableName1, Integer key1, Integer tablename2, Integer key2) {
        byte[] res = bftMap.getEntries(tableName1, key1, tablename2, key2);
        if (res == null)
            return null;
        return res;
    }
}
