package demo.bftmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import parallelism.ParallelServiceReplica;

public final class BFTMapServerMP extends DefaultSingleRecoverable implements SingleExecutable, Serializable {

    private static final transient Logger logger = LoggerFactory.getLogger(BFTMapServerMP.class);

    private static int CPperiod;
    private int interval;
    private float maxTp = -1;
    private boolean context;
    MapOfMapsMP tableMap = null;
    private int iterations = 0;
    private long throughputMeasurementStartTime = System.currentTimeMillis();
    private long start = 0;
    private ServiceReplica replica;
    boolean partition;
    public int workers;
    private PrintWriter pw;
    private boolean closed = false;

    public BFTMapServerMP(int id, int interval, int maxThreads, int minThreads, int initThreads, int entries,
            boolean context, boolean cbase, boolean partition) throws IOException, ClassNotFoundException {

        logger.info("Initializing BFTMapServerMP");
        if (initThreads <= 0) {
            logger.info("Replica in sequential execution model.");
            tableMap = new MapOfMapsMP();
            replica = new ServiceReplica(id, this, this);
            this.workers = initThreads;
        } else if (cbase) {
            logger.info("Replica in parallel execution model (CBASE).");

        } else {
            logger.info("Replica in parallel execution model.");
            tableMap = new MapOfMapsMP();

        }
        this.partition = partition;
        this.interval = interval;
        this.context = context;
        for (int i = 0; i < initThreads; i++) {
            tableMap.addTable(i, new TreeMap<Integer, byte[]>());
            for (int j = 0; j < ((981760 * entries) / 1024) / initThreads; j++) {
                tableMap.getTable(i).put(j, ByteBuffer.allocate(1024).putInt(2).array());
            }
        }
        logger.info("Number of tables = {}", tableMap.getNumOfTables());
        for (int j = 0; j < initThreads; j++) {
            logger.info("Table {} has size of {} entries", j, tableMap.getSize(j));
        }
        replica = new ParallelServiceReplica(id, this, this, initThreads, CPperiod, partition);

        logger.info("Server initialization complete!");
    }

    public byte[] executeOrdered(byte[] command, MessageContext msgCtx) {
        // logger.info("ckp");
        return execute(command, msgCtx);
    }

    public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
        return execute(command, msgCtx);
    }

    long lastChange = 0;

    public byte[] execute(byte[] command, MessageContext msgCtx) {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(command);
            ByteArrayOutputStream out = null;
            byte[] reply = null;
            int cmd = new DataInputStream(in).readInt();
            long start = System.nanoTime();
            logger.debug("Executing command type: {}", cmd);
            switch (cmd) {
                case BFTMapRequestType.PUT:
                    Integer tableName = new DataInputStream(in).readInt();
                    Integer key = new DataInputStream(in).readInt();
                    String value = new DataInputStream(in).readUTF();
                    byte[] valueBytes = ByteBuffer.allocate(1024).array();
                    byte[] ret = tableMap.addData(tableName, key, valueBytes);
                    if (ret == null) {
                        ret = new byte[0];
                    }
                    reply = valueBytes;
                    break;
                case BFTMapRequestType.REMOVE:
                    tableName = new DataInputStream(in).readInt();
                    key = new DataInputStream(in).readInt();
                    valueBytes = tableMap.removeEntry(tableName, key);
                    value = new String(valueBytes);
                    out = new ByteArrayOutputStream();
                    new DataOutputStream(out).writeBytes(value);
                    reply = out.toByteArray();
                    break;
                case BFTMapRequestType.TAB_CREATE:
                    tableName = new DataInputStream(in).readInt();
                    // ByteArrayInputStream in1 = new ByteArrayInputStream(command);
                    ObjectInputStream objIn = new ObjectInputStream(in);
                    Map<Integer, byte[]> table = null;
                    try {
                        // logger.info("TABLE CREATED!!!!!");
                        table = (Map<Integer, byte[]>) objIn.readObject();
                    } catch (ClassNotFoundException ex) {
                        logger.error("Error on create table operation", ex.getCause());
                        throw new RuntimeException(ex);
                    }
                    Map<Integer, byte[]> tableCreated = tableMap.addTable(tableName, table);
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutputStream objOut = new ObjectOutputStream(bos);
                    objOut.writeObject(tableCreated);
                    objOut.close();
                    in.close();
                    reply = bos.toByteArray();
                    break;
                case BFTMapRequestType.TAB_REMOVE:
                    tableName = new DataInputStream(in).readInt();
                    table = tableMap.removeTable(tableName);
                    bos = new ByteArrayOutputStream();
                    objOut = new ObjectOutputStream(bos);
                    objOut.writeObject(table);
                    objOut.close();
                    objOut.close();
                    reply = bos.toByteArray();
                    break;
                case BFTMapRequestType.SIZE_TABLE:
                    int size1 = tableMap.getNumOfTables();
                    out = new ByteArrayOutputStream();
                    new DataOutputStream(out).writeInt(size1);
                    reply = out.toByteArray();
                    break;
                case BFTMapRequestType.GET:
                    tableName = new DataInputStream(in).readInt();
                    key = new DataInputStream(in).readInt();
                    valueBytes = tableMap.getEntry(tableName, key);
                    value = new String(valueBytes);
                    out = new ByteArrayOutputStream();
                    new DataOutputStream(out).writeBytes(value);
                    reply = out.toByteArray();
                    break;
                case BFTMapRequestType.SIZE:
                    Integer tableName2 = new DataInputStream(in).readInt();
                    int size = tableMap.getSize(tableName2);
                    out = new ByteArrayOutputStream();
                    new DataOutputStream(out).writeInt(size);
                    reply = out.toByteArray();
                    break;
                case BFTMapRequestType.CHECK:
                    tableName = new DataInputStream(in).readInt();
                    key = new DataInputStream(in).readInt();
                    valueBytes = tableMap.getEntry(tableName, key);
                    boolean entryExists = valueBytes != null;
                    out = new ByteArrayOutputStream();
                    new DataOutputStream(out).writeBoolean(entryExists);
                    reply = out.toByteArray();
                    break;
                case BFTMapRequestType.TAB_CREATE_CHECK:
                    tableName = new DataInputStream(in).readInt();
                    table = tableMap.getTable(tableName);
                    boolean tableExists = (table != null);
                    out = new ByteArrayOutputStream();
                    new DataOutputStream(out).writeBoolean(tableExists);
                    reply = out.toByteArray();
                    break;
                case BFTMapRequestType.CKP:
                    String part = new DataInputStream(in).readUTF();
                    String[] partitions = part.split("#");
                    int[] particoes = new int[partitions.length];
                    for (int i = 0; i < partitions.length; i++) {
                        particoes[i] = Integer.parseInt(partitions[i]);
                    }

                    reply = getSnapshot(particoes);
                    break;
                case BFTMapRequestType.PUT12:
                    Integer tableNamea = new DataInputStream(in).readInt();
                    Integer keya = new DataInputStream(in).readInt();
                    Integer tableNameb = new DataInputStream(in).readInt();
                    Integer keyb = new DataInputStream(in).readInt();

                    byte[] valueBytes1 = ByteBuffer.allocate(1024).array();
                    reply = tableMap.addData(tableNamea, keya, valueBytes1);
                    reply = tableMap.addData(tableNameb, keyb, valueBytes1);
                    break;
                case BFTMapRequestType.RECOVERER:
                    ObjectInputStream is = new ObjectInputStream(in);
                    installSnapshot(command);
                    break;
                case BFTMapRequestType.SENDER:
                    sendState();
                    break;
                default:
                    throw new RuntimeException("Unmapped operation!");
            }
            logger.debug("Took {} ns to execute command type: {}", (System.nanoTime() - start), cmd);
            return reply;
        } catch (Exception ex) {
            logger.error("Error on execute operation", ex.getCause());
            throw new RuntimeException(ex);
        }

    }

    public byte[] getSnapshot(int[] particoes) {
        logger.info("Getting snapshot {}", particoes.length);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            for (int i = 0; i < particoes.length; i++) {
                out.writeObject(tableMap.getTable(particoes[i]));
            }
            out.flush();
            out.close();
            byte[] state = bos.toByteArray();
            bos.flush();
            bos.close();
            return state;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void computeStatistics(MessageContext msgCtx) {
        iterations++;

        float tp = -1;
        if (iterations % interval == 0) {
            if (context) {
                logger.info("--- (Context)  iterations: {}, // regency: {} // consensus: {} ---", iterations,
                        msgCtx.getRegency(), msgCtx.getConsensusId());
            }

            logger.info("--- Measurements after {} ops ({} samples) ---", iterations, interval);

            tp = (float) (interval * 1000 / (float) (System.currentTimeMillis() - throughputMeasurementStartTime));

            if (tp > maxTp) {
                maxTp = tp;
            }

            int now = (int) ((System.currentTimeMillis() - start) / 1000);

            if (now < 3000) {

                // logger.info("****************THROUGHPUT: "+now+" "+tp);
                if (replica instanceof ParallelServiceReplica) {

                    pw.println(now + " " + tp + " " + ((ParallelServiceReplica) replica).getNumActiveThreads());
                    // logger.info("*******************THREADS: "+now+"
                    // "+((ParallelServiceReplica)replica).getNumActiveThreads());
                } else {
                    pw.println(now + " " + tp);
                }
                pw.flush();

            } else if (!closed) {

                pw.flush();

                pw.close();

                closed = true;
            }

            if (replica instanceof ParallelServiceReplica) {
                logger.info("Active Threads = {}", ((ParallelServiceReplica) replica).getNumActiveThreads());
            }

            throughputMeasurementStartTime = System.currentTimeMillis();
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        if (args.length < 6) {
            logger.error(
                    "Usage: ... BFTMapServerMP <processId> <measurement interval> <Num threads> <initial entries> <checkpoint period>  <particionado?>");
            System.exit(-1);
        }

        int processId = Integer.parseInt(args[0]);
        int interval = Integer.parseInt(args[1]);
        int minNT = Integer.parseInt(args[2]);
        int initialNT = Integer.parseInt(args[2]);
        int maxNT = Integer.parseInt(args[2]);
        int entries = Integer.parseInt(args[3]);
        boolean context = false;
        boolean cbase = false;
        boolean partition = Boolean.parseBoolean(args[5]);
        CPperiod = Integer.parseInt(args[4]);
        new BFTMapServerMP(processId, interval, maxNT, minNT, initialNT, entries, context, cbase, partition);
    }

    private void sendState() {

    }

    @Override
    public void installSnapshot(byte[] bytes) {
        logger.info("Installing snapshot...");
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        ObjectInputStream is;

        try {
            is = new ObjectInputStream(in);
            is.readInt();
            tableMap.addTable(is.readInt(), (Map<Integer, byte[]>) is.readObject());
        } catch (IOException | ClassNotFoundException ex) {
            logger.error("Error installing snapshot", ex.getCause());
            throw new RuntimeException(ex);
        }
        logger.info("Snapshot installed");
    }

    @Override
    public byte[] getSnapshot() {
        logger.info("Getting snapshot...");
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this.tableMap);
            out.flush();
            byte[] yourBytes = bos.toByteArray();
            logger.info("Got the snapshot");
            return yourBytes;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public byte[] appExecuteOrdered(byte[] bytes, MessageContext mc) {
        throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
                                                                       // Tools | Templates.
    }

    @Override
    public byte[] appExecuteUnordered(byte[] bytes, MessageContext mc) {
        throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
                                                                       // Tools | Templates.
    }

    @Override
    public void noOp(int i, byte[][] bytes, MessageContext[] mcs) {
        throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
                                                                       // Tools | Templates.
    }

}