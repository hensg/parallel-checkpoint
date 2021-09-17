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
import java.util.logging.Level;
import java.util.logging.Logger;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import parallelism.ParallelServiceReplica;

public final class BFTMapServerMP extends DefaultSingleRecoverable implements SingleExecutable, Serializable {
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

        if (initThreads <= 0) {
            System.out.println("Replica in sequential execution model.");
            tableMap = new MapOfMapsMP();
            replica = new ServiceReplica(id, this, this);
            this.workers = initThreads;
        } else if (cbase) {
            System.out.println("Replica in parallel execution model (CBASE).");

        } else {
            System.out.println("Replica in parallel execution model.");
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
        System.out.println("numOfTables =" + tableMap.getNumOfTables());
        for (int j = 0; j < initThreads; j++) {
            System.out.println("table " + j + " = " + tableMap.getSize(j));
        }
        replica = new ParallelServiceReplica(id, this, this, initThreads, CPperiod, partition);

        System.out.println("Server initialization complete!");
    }

    public byte[] executeOrdered(byte[] command, MessageContext msgCtx) {
        // System.out.println("ckp");
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
                        // System.out.println("TABLE CREATED!!!!!");
                        table = (Map<Integer, byte[]>) objIn.readObject();
                    } catch (ClassNotFoundException ex) {
                        Logger.getLogger(bftsmart.demo.bftmap.BFTMapServer.class.getName()).log(Level.SEVERE, null, ex);
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
                    System.out.println("morreu aki?");
                    int size1 = tableMap.getNumOfTables();
                    // System.out.println("Size " + size1);
                    out = new ByteArrayOutputStream();
                    new DataOutputStream(out).writeInt(size1);
                    reply = out.toByteArray();
                    break;
                case BFTMapRequestType.GET:
                    tableName = new DataInputStream(in).readInt();
                    // System.out.println("tablename: " + tableName);
                    key = new DataInputStream(in).readInt();
                    // System.out.println("Key received: " + key);
                    valueBytes = tableMap.getEntry(tableName, key);
                    value = new String(valueBytes);
                    // System.out.println("The value to be get is: " + value);
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
                    // System.out.println("Table Key received: " + key);
                    valueBytes = tableMap.getEntry(tableName, key);
                    boolean entryExists = valueBytes != null;
                    out = new ByteArrayOutputStream();
                    new DataOutputStream(out).writeBoolean(entryExists);
                    reply = out.toByteArray();
                    break;
                case BFTMapRequestType.TAB_CREATE_CHECK:
                    tableName = new DataInputStream(in).readInt();
                    // System.out.println("Table of Table Key received: " + tableName);
                    table = tableMap.getTable(tableName);
                    boolean tableExists = (table != null);
                    // System.out.println("Table exists: " + tableExists);
                    out = new ByteArrayOutputStream();
                    new DataOutputStream(out).writeBoolean(tableExists);
                    reply = out.toByteArray();
                    break;
                case BFTMapRequestType.CKP:
                    String part = new DataInputStream(in).readUTF();
                    String[] partitions = part.split("#");
                    // System.out.println("part = "+part);

                    int[] particoes = new int[partitions.length];
                    for (int i = 0; i < partitions.length; i++) {
                        particoes[i] = Integer.parseInt(partitions[i]);
                        // System.out.println("partitions = "+particoes[i]);
                    }

                    reply = getSnapshot(particoes);
                    return reply;
                case BFTMapRequestType.PUT12:
                    Integer tableNamea = new DataInputStream(in).readInt();
                    Integer keya = new DataInputStream(in).readInt();
                    // String valuea = new DataInputStream(in).readUTF();
                    Integer tableNameb = new DataInputStream(in).readInt();
                    Integer keyb = new DataInputStream(in).readInt();

                    byte[] valueBytes1 = ByteBuffer.allocate(1024).array();
                    reply = tableMap.addData(tableNamea, keya, valueBytes1);
                    reply = tableMap.addData(tableNameb, keyb, valueBytes1);
                    return reply;
                case BFTMapRequestType.RECOVERER:
                    ObjectInputStream is = new ObjectInputStream(in);
                    installSnapshot(command);

                    return reply;
                case BFTMapRequestType.SENDER:
                    sendState();
                    return reply;
                default:
                    // System.out.println("operation = "+cmd);
                    break;
            }
            return reply;
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(BFTMapServerMP.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

    }

    public byte[] getSnapshot(int[] particoes) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        System.out.println("table amount = " + particoes.length);
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
            // System.out.println("STATE");
            return state;
        } catch (Exception ex) {
            // ignore close exception
            return null;
        }
    }

    public void computeStatistics(MessageContext msgCtx) {
        iterations++;

        float tp = -1;
        if (iterations % interval == 0) {
            if (context) {
                System.out.println("--- (Context)  iterations: " + iterations + " // regency: " + msgCtx.getRegency()
                        + " // consensus: " + msgCtx.getConsensusId() + " ---");
            }

            System.out.println("--- Measurements after " + iterations + " ops (" + interval + " samples) ---");

            tp = (float) (interval * 1000 / (float) (System.currentTimeMillis() - throughputMeasurementStartTime));

            if (tp > maxTp) {
                maxTp = tp;
            }

            int now = (int) ((System.currentTimeMillis() - start) / 1000);

            if (now < 3000) {

                // System.out.println("****************THROUGHPUT: "+now+" "+tp);
                if (replica instanceof ParallelServiceReplica) {

                    pw.println(now + " " + tp + " " + ((ParallelServiceReplica) replica).getNumActiveThreads());
                    // System.out.println("*******************THREADS: "+now+"
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
                System.out.println("Active Threads = " + ((ParallelServiceReplica) replica).getNumActiveThreads()
                        + " in sec: " + now);
            }

            throughputMeasurementStartTime = System.currentTimeMillis();
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        if (args.length < 6) {
            System.out.println(
                    "Usage: ... ListServer <processId> <measurement interval> <Num threads> <initial entries> <checkpoint period>  <particionado?>");
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
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        ObjectInputStream is;

        try {
            is = new ObjectInputStream(in);
            is.readInt();
            tableMap.addTable(is.readInt(), (Map<Integer, byte[]>) is.readObject());
        } catch (IOException ex) {
            Logger.getLogger(BFTMapServerMP.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(BFTMapServerMP.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("Snapshot intalled at time = " + System.nanoTime());
    }

    @Override
    public byte[] getSnapshot() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this.tableMap);
            out.flush();
            byte[] yourBytes = bos.toByteArray();
            return yourBytes;
        } catch (Exception ex) {
            System.out.println("ERROR");
        }
        return null;
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