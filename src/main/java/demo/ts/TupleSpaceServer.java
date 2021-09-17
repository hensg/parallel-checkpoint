/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package demo.ts;

import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.StateManager;
import bftsmart.statemanagement.strategy.StandardStateManager;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.tom.util.Storage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import parallelism.ParallelServiceReplica;
import parallelism.reconfiguration.LazyPolicy;

/**
 *
 * @author alchieri
 */
//public class TupleSpaceServer implements SingleExecutable, Recoverable {
public class TupleSpaceServer implements SingleExecutable {

    private int interval;
    private float maxTp = -1;
    private boolean context;

    //private byte[] state;
    private int iterations = 0;
    private long throughputMeasurementStartTime = System.currentTimeMillis();

   
    protected ServiceReplica replica;
    //private ReplicaContext replicaContext;

    //private StateManager stateManager;
    private Map<Integer, List<Tuple>> tuplesBag = new TreeMap<Integer, List<Tuple>>();

    private int myId;
    private PrintWriter pw;
     private long start = 0;

    public TupleSpaceServer(int id, int interval, int nimT, int initT, int maxT, int entries, boolean context) {
        myId = id;
        initReplica(nimT, initT, maxT, id);

        this.interval = interval;
        this.context = context;
        /*
        this.state = new byte[stateSize];

        for (int i = 0; i < stateSize; i++) {
            state[i] = (byte) i;
        }*/

        /*totalLatency = new Storage(interval);
        consensusLatency = new Storage(interval);
        preConsLatency = new Storage(interval);
        posConsLatency = new Storage(interval);
        proposeLatency = new Storage(interval);
        writeLatency = new Storage(interval);
        acceptLatency = new Storage(interval);*/

        for (int i = 0; i < entries; i++) {
            for (int j = 1; j <= 10; j++) {
                Object[] f = new Object[j];
                for (int x = 0; x < f.length; x++) {
                    f[x] = new String("Este Campo Possui Os Dados Do Campo iiiiiiiiiiii:" + i);
                    //System.out.println("Tamanho "+x+" e i "+j+" igual a " +f[x].toString().length());
                }
                out(Tuple.createTuple(f));
            }

        }
        for (int j = 1; j <= 10; j++) {
            System.out.println("tuples" + j + "fields: " + getTuplesBag(j).size());
        }

        try {
            File f = new File("resultado_" + id + ".txt");
            FileWriter fw = new FileWriter(f);
            pw = new PrintWriter(fw);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        System.out.println("Server initialization complete!");
    }

    protected void initReplica(int minT, int initT, int maxT, int id) {

        if (initT == 0) {
            System.out.println("Replica in sequential execution model.");
            replica = new ServiceReplica(id, this, null);
        } else {
            System.out.println("Replica in parallel execution model.");

            //replica = new ParallelServiceReplica(id, this, null, numThreads);
            //replica = new ParallelServiceReplica(id, this, null, minT, initT, maxT, new LazyPolicy());

        }

    }

    public byte[] executeOrdered(byte[] command, MessageContext msgCtx) {
        return execute(command, msgCtx);
    }

    public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
        return execute(command, msgCtx);
    }

    public byte[] execute(byte[] command, MessageContext msgCtx) {

         if (start == 0) {
            
            start = System.currentTimeMillis();
            throughputMeasurementStartTime = start;
        }
        
        computeStatistics(msgCtx);

        try {
            ByteArrayInputStream in = new ByteArrayInputStream(command);
            ByteArrayOutputStream out = null;
            byte[] reply = null;
            int cmd = new DataInputStream(in).readInt();
            switch (cmd) {
                //operations on the list
                case BFTTupleSpace.OUT:
                    Tuple t = (Tuple) new ObjectInputStream(in).readObject();
                    //System.out.println("add received: " + t);
                    out(t);
                    //boolean ret = true;
                    out = new ByteArrayOutputStream();
                    ObjectOutputStream out1 = new ObjectOutputStream(out);
                    out1.writeBoolean(true);

                    out1.flush();
                    out.flush();
                    reply = out.toByteArray();
                    break;
                case BFTTupleSpace.RDP:
                    t = (Tuple) new ObjectInputStream(in).readObject();
                    Tuple read = rdp(t);
                    out = new ByteArrayOutputStream();

                    out1 = new ObjectOutputStream(out);
                    out1.writeObject(read);
                    out1.flush();
                    out.flush();
                    reply = out.toByteArray();
                    break;
                case BFTTupleSpace.INP:
                    t = (Tuple) new ObjectInputStream(in).readObject();
                    //Tuple removed = inp(t);
                    //TODO:ajeitar
                    Tuple removed = rdp(t);

                    out = new ByteArrayOutputStream();
                    out1 = new ObjectOutputStream(out);
                    out1.writeObject(removed);
                    out1.flush();
                    out.flush();
                    reply = out.toByteArray();
                    break;
                case BFTTupleSpace.CAS:
                    ObjectInputStream ois = new ObjectInputStream(in);
                    t = (Tuple) ois.readObject();
                    boolean ret = false;
                    if (findMatching(t, false) == null) {
                        t = (Tuple) ois.readObject();
                        out(t);
                        ret = true;
                    }

                    out = new ByteArrayOutputStream();
                    out1 = new ObjectOutputStream(out);
                    out1.writeBoolean(ret);

                    out1.flush();
                    out.flush();
                    reply = out.toByteArray();
                    break;
            }
            return reply;
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(TupleSpaceServer.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
            return null;
        }
    }

    public void computeStatistics(MessageContext msgCtx) {

       
        iterations++;
    
        float tp = -1;
        if (iterations % interval == 0) {
            if (context) {
                System.out.println("--- (Context)  iterations: " + iterations + " // regency: " + msgCtx.getRegency() + " // consensus: " + msgCtx.getConsensusId() + " ---");
            }

            System.out.println("--- Measurements after " + iterations + " ops (" + interval + " samples) ---");

            tp = (float) (interval * 1000 / (float) (System.currentTimeMillis() - throughputMeasurementStartTime));

            if (tp > maxTp) {
                maxTp = tp;
            }

            int now = (int) ((System.currentTimeMillis() - start) / 1000);
            System.out.println("Throughput = " + tp + " operations/sec at sec: "+now+" (Maximum observed: " + maxTp + " ops/sec)");

             
            if (replica instanceof ParallelServiceReplica) {

                pw.println(now + " " + tp + " " + ((ParallelServiceReplica) replica).getNumActiveThreads());
               
            } else {
                pw.println(now + " " + tp);
            }
            pw.flush();

            throughputMeasurementStartTime = System.currentTimeMillis();
        }

    }

    private void out(Tuple tuple) {
        getTuplesBag(tuple.getFields().length).add(tuple);
    }

    private Tuple rdp(Tuple template) {
        return findMatching(template, false);
    }

    private Tuple inp(Tuple template) {
        return findMatching(template, true);
    }

    private Tuple findMatching(Tuple template, boolean remove) {
        List<Tuple> bag = getTuplesBag(template.getFields().length);
        for (ListIterator<Tuple> i = bag.listIterator(); i.hasNext();) {
            Tuple tuple = i.next();
            if (match(tuple, template)) {
                if (remove) {
                    i.remove();
                }
                return tuple;
            }
        }

        return null;
    }

    protected boolean match(Tuple tuple, Tuple template) {
        Object[] tupleFields = tuple.getFields();
        Object[] templateFields = template.getFields();

        int n = tupleFields.length;

        if (n != templateFields.length) {
            return false;
        }

        for (int i = 0; i < n; i++) {
            if (templateFields[i] == tupleFields[i]) {
                return true;
            }

            if (templateFields[i] == null || tupleFields[i] == null) {
                return false;
            }

            if (!templateFields[i].equals(BFTTupleSpace.WILDCARD) && !templateFields[i].equals(tupleFields[i])) {
                return false;
            }
        }
        return true;
    }

    private List getTuplesBag(int i) {
        List ret = tuplesBag.get(i);
        if (ret == null) {
            synchronized (this) {
                ret = tuplesBag.get(i);
                if (ret == null) {
                    ret = new LinkedList<Tuple>();
                    tuplesBag.put(i, ret);
                }
            }
        }
        return ret;
    }

    public static void main(String[] args) {

        if (args.length < 7) {
            System.out.println("Usage: ... TupleSpaceServer <processId> <measurement interval> <minNum threads> <initialNum threads> <maxNum threads> <initial entries> <context?>");
            System.exit(-1);
        }

        int processId = Integer.parseInt(args[0]);
        int interval = Integer.parseInt(args[1]);
        int mint = Integer.parseInt(args[2]);
        int initt = Integer.parseInt(args[3]);
        int maxt = Integer.parseInt(args[4]);

        int entries = Integer.parseInt(args[5]);

        boolean context = Boolean.parseBoolean(args[6]);

        new TupleSpaceServer(processId, interval, mint, initt, maxt, entries, context);
    }

}
