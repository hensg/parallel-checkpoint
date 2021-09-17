package parallelism.scheduler;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import demo.bftmap.BFTMapRequestType;
import parallelism.ClassToThreads;
import parallelism.EarlySchedulerMapping;
import parallelism.HibridClassToThreads;
import parallelism.MessageContextPair;
import parallelism.ParallelMapping;

/**
 *
 * @author eduardo
 */
public class ParallelScheduler implements Scheduler {

    protected ParallelMapping mapping;
    private HibridClassToThreads[] cts;
    public HashMap<Integer, HibridClassToThreads> classes;
    private int cmds;
    private int CPperiod;
    private int starter;
    private int[][] conf;
    private int workers;
    public int x;

    public ParallelScheduler(int repID, int numberWorkers, int period) {
        EarlySchedulerMapping e = new EarlySchedulerMapping();
        this.cts = e.generateMappings(numberWorkers);
        System.out.println("cts size = " + this.cts.length);
        this.classes = new HashMap<Integer, HibridClassToThreads>();
        for (int i = 0; i < cts.length; i++) {
            try {
                if (cts[i].tIds.length <= 2)
                    this.classes.put(cts[i].classId, cts[i]);
            } catch (NullPointerException ex) {
                System.out.println("error for i = " + i);
            }
        }
        this.mapping = new ParallelMapping(numberWorkers, cts);
        this.cmds = 0;
        this.CPperiod = period;
        this.conf = new int[numberWorkers][numberWorkers];
        for (int i = 0; i < numberWorkers; i++) {
            for (int j = 0; j < numberWorkers; j++) {
                if (i == j)
                    conf[i][j] = 1;
                else
                    conf[i][j] = 0;
                System.out.print(conf[i][j] + "");
            }
            System.out.println("");
        }
        this.starter = repID;
        this.workers = numberWorkers;
    }

    @Override
    public int getNumWorkers() {
        return this.mapping.getNumWorkers();
    }

    @Override
    public ParallelMapping getMapping() {
        return mapping;
    }

    public HashMap<Integer, HibridClassToThreads> getClasses() {
        return this.classes;
    }

    private void clearConf(List<Integer> conflict) {
        int i = 0;
        int j = 0;
        int threads = this.workers;
        // System.out.println("threads = "+threads);
        int conflict_size = conflict.size();
        for (i = 0; i < conflict_size; i++) {
            // System.out.println("conflict get(i) = "+conflict.get(i));
            for (j = 0; j < threads; j++) {
                if (conflict.get(i) == j)
                    this.conf[conflict.get(i)][j] = 1;
                else
                    this.conf[conflict.get(i)][j] = 0;
            }
        }
    }

    public List<Integer> conflictMapping(int[][] conf, int threads, int starter) {
        int i = 0;
        // System.out.println("starter = "+starter);
        // for(int n=0;n<threads;n++){
        // for(int m=0;m<threads;m++){
        // System.out.print(conf[n][m]);
        // }
        // System.out.println("");
        // }
        List<Integer> conflict = new LinkedList<Integer>();
        for (i = 0; i < threads; i++) {
            if (conf[starter][i] == 1)
                conflict.add(i);
        }

        for (i = 0; i < conflict.size(); i++) {
            if (conflict.get(i) != starter) {
                for (int j = 0; j < threads; j++) {
                    if (conf[conflict.get(i)][j] == 1 && !conflict.contains(j)) {
                        conflict.add(j);
                        for (int k = 0; k < threads; k++) {
                            if (conf[j][k] == 1 && !conflict.contains(j)) {
                                conflict.add(k);
                            }
                        }
                    }
                }
            }

        }
        clearConf(conflict);

        // ver no código do alchieri como são construidas as classes de conflito
        return conflict;
    }

    @Override
    public void scheduleReplicaReconfiguration() {
        TOMMessage reconf = new TOMMessage(0, 0, 0, 0, null, 0, TOMMessageType.ORDERED_REQUEST,
                ParallelMapping.CONFLICT_RECONFIGURATION);
        MessageContextPair m = new MessageContextPair(reconf, ParallelMapping.CONFLICT_RECONFIGURATION, -1, null, null);
        BlockingQueue[] q = this.getMapping().getAllQueues();
        try {
            for (BlockingQueue q1 : q) {
                q1.put(m);
            }
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }

    @Override
    public void schedule(MessageContextPair request) {
        HibridClassToThreads ct = this.mapping.getClass(request.classId);
        // System.out.println("cmds = "+cmds);
        // System.out.println("request class id = "+request.classId);
        if (ct == null) {
            // TRATAR COMO CONFLICT ALL
            // criar uma classe que sincroniza tudo
            System.err.println("CLASStoTHREADs MAPPING NOT FOUND");
        }
        // System.out.println("queues length = "+ct.queues.length);
        if (ct.type == ClassToThreads.CONC) {// conc
            ct.queues[ct.threadIndex].add(request);
            ct.threadIndex = (ct.threadIndex + 1) % ct.queues.length;
        } else { // sync
            for (Queue q : ct.queues) {
                q.add(request);
            }
        }
        cmds++;
        // set conflict matrix
        ct = this.getMapping().getClass(request.request.groupId);
        int executor = this.getMapping().getExecutorThread(request.request.groupId);
        for (int k = 0; k < ct.tIds.length; k++) {
            conf[executor][ct.tIds[k]] = 1;
            conf[ct.tIds[k]][executor] = 1;
        }

        if (cmds % CPperiod == 0) { // create CP request
            List<Integer> conflict = conflictMapping(conf, workers, starter % workers);
            Collections.sort(conflict);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < conflict.size(); i++) {
                sb.append(conflict.get(i));
                sb.append('#');

            }
            try {
                dos.writeInt(BFTMapRequestType.CKP);
                dos.writeUTF(sb.toString());
                // System.out.println("string for 1 partition = "+sb.toString());
                dos.writeInt(request.request.getSequence());
            } catch (IOException ex) {
                Logger.getLogger(ParallelScheduler.class.getName()).log(Level.SEVERE, null, ex);
            }
            byte[] b = out.toByteArray();
            TOMMessage req = new TOMMessage(1, 1, request.m.getConsensusId() + 1, b, 1);
            if (conflict.size() == 1)
                sb.append('S');
            req.groupId = sb.toString().hashCode();
            // System.out.println("after check = "+sb.toString());
            MessageContextPair cp = new MessageContextPair(req, req.groupId, 0, b, null);
            // System.out.println("CP.CLASS ID = "+cp.classId);
            HibridClassToThreads CP_class = this.classes.get(cp.classId);
            if (CP_class == null) {
                long now = System.nanoTime();
                // System.out.println("cp null id = "+cp.classId);
                int[] ids = new int[conflict.size()];
                for (int i = 0; i < conflict.size(); i++) {
                    ids[i] = conflict.get(i); // se precisar criar a classe

                }
                // System.out.println("total ids length = "+ids.length);
                CP_class = new HibridClassToThreads(sb.toString().hashCode(), HibridClassToThreads.SYNC, ids);
                this.mapping.setQueue(CP_class);
                this.classes.put(sb.toString().hashCode(), CP_class);
                // System.out.println("total overhead for creating class (micro s) =
                // "+(double)((System.nanoTime()-now)/1000.0));
            }
            // System.out.println("antes = "+CP_class.type);
            // CP_class.type=HibridClassToThreads.SYNC;
            // System.out.println("depois = "+CP_class.type);
            for (Queue q : CP_class.queues) {
                q.add(cp);
            }
            starter++;
        }
    }

}
