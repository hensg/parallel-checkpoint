package parallelism.scheduler;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger logger = LoggerFactory.getLogger(ParallelScheduler.class);

    protected ParallelMapping mapping;
    private HibridClassToThreads[] cts;
    public HashMap<Integer, HibridClassToThreads> classes;
    private int cmds;
    private int CPperiod;
    private int starter;
    private int[][] conf;
    private int workers;
    public int x;
    private int extraCPs = 0;

    public ParallelScheduler(int repID, int numberWorkers, int period) {
        EarlySchedulerMapping e = new EarlySchedulerMapping();
        this.cts = e.generateMappings(numberWorkers);
        int[] ids = new int[numberWorkers];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = i;
        }
        int lastCts = this.cts.length;
        HibridClassToThreads[] newCts = new HibridClassToThreads[lastCts + 2];
        System.arraycopy(this.cts, 0, newCts, 0, lastCts);
        newCts[lastCts++] = new HibridClassToThreads(ParallelMapping.CONC_ALL, ClassToThreads.CONC, ids);
        newCts[lastCts++] = new HibridClassToThreads(ParallelMapping.SYNC_ALL, ClassToThreads.SYNC, ids);
        this.cts = newCts;

        logger.info("Mapping class to threads generated, size = {}", this.cts.length);
        this.classes = new HashMap<Integer, HibridClassToThreads>();
        for (int i = 0; i < cts.length; i++) {
            logger.info("Generated hash {} for op {}", this.cts[i].classId, cts[i]);
            try {
                if (cts[i].tIds.length <= 2) {
                    this.classes.put(cts[i].classId, cts[i]);
                    logger.debug("Classes with id {} to thread {} of type {}",
                            cts[i].classId, i,
                            this.classes.get(cts[i].classId).type);
                }
            } catch (NullPointerException ex) {
                logger.info("error for i = {}", i);
                System.exit(-1);
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
                logger.info(conf[i][j] + "");
            }
            logger.info("");
        }
        this.starter = repID;
        this.workers = numberWorkers;

        logger.info("Parallel logging initialized with {} workers", this.workers);
        logger.info("Classes to threads generated: {}", this.classes.size());
        StringBuilder sb = new StringBuilder();
        for (Entry<Integer, HibridClassToThreads> entry : this.classes.entrySet()) {
            sb.append(entry.getKey() + "=" + entry.getValue());
        }

        logger.info("Mapping: {}", sb.toString());
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
        // logger.info("threads = "+threads);
        int conflict_size = conflict.size();
        for (i = 0; i < conflict_size; i++) {
            // logger.info("conflict get(i) = "+conflict.get(i));
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
        // logger.info("starter = "+starter);
        // for(int n=0;n<threads;n++){
        // for(int m=0;m<threads;m++){
        // logger.info(conf[n][m]);
        // }
        // logger.info("");
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
        } catch (InterruptedException ex) {
            logger.error("Failed to schedule replica reconfiguration", ex);
            throw new RuntimeException("Failed to schedule replica reconfiguration", ex);
        }
    }

    @Override
    public void schedule(MessageContextPair request) {
        HibridClassToThreads ct = this.mapping.getClass(request.classId);
        if (ct == null) {
            // TRATAR COMO CONFLICT ALL
            // criar uma classe que sincroniza tudo
            logger.error("Class to threads mapping not found, classId: {}", request.classId);
            throw new RuntimeException();
        }
        if (ct.type == ClassToThreads.CONC) {// conc
            logger.info("Added request to queue of thread {}", ct.threadIndex);
            ct.queues[ct.threadIndex].add(request);
            ct.threadIndex = (ct.threadIndex + 1) % ct.queues.length;
        } else { // sync
            for (Queue q : ct.queues) {
                logger.info("Added request to queue {}", q);
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

        // FIXME: Adiantar o checkpoint para bater com o do scheduler serial.
        // Cuidar com o adiantamento causado por conflito que pode acabar
        // gerando mais CP
        if (cmds % (CPperiod / workers) == 0) { // create cp request
            logger.info("Time for checkpointing with {} commands executed, extra CPs {}", cmds, extraCPs);
            if (extraCPs > 0) {
                extraCPs -= 1;
                return;
            }

            List<Integer> conflict = conflictMapping(conf, workers, starter % workers);
            Collections.sort(conflict);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < conflict.size(); i++) {
                sb.append(conflict.get(i));
                sb.append('#');
            }
            if (conflict.size() > 1) {
                logger.info("Conflict checkpointing: {}", sb);
            }

            logger.info("Checkpoint request created for partitions: {}, conflict list {}", sb.toString(), conflict);

            try {
                dos.writeInt(BFTMapRequestType.CKP);
                dos.writeUTF(sb.toString());
                dos.writeInt(request.request.getSequence());
            } catch (IOException ex) {
                logger.error("Failed to write to data output stream", ex);
                throw new RuntimeException("Failed to write to data output stream", ex);
            }
            byte[] b = out.toByteArray();
            TOMMessage req = new TOMMessage(1, 1, request.m.getConsensusId() + 1, b, 1);
            if (conflict.size() == 1)
                sb.append('S');
            req.groupId = sb.toString().hashCode();
            MessageContextPair cp = new MessageContextPair(req, req.groupId, 0, b, null);
            HibridClassToThreads CP_class = this.classes.get(cp.classId);
            if (CP_class == null) {
                int[] ids = new int[conflict.size()];
                for (int i = 0; i < conflict.size(); i++) {
                    ids[i] = conflict.get(i); // se precisar criar a classe

                }
                CP_class = new HibridClassToThreads(sb.toString().hashCode(), HibridClassToThreads.SYNC, ids);
                this.mapping.setQueue(CP_class);
                this.classes.put(sb.toString().hashCode(), CP_class);
            }

            logger.info("CP Class Queue lenght is {}", CP_class.queues.length);
            if (CP_class.queues.length > 1) {
                extraCPs += (CP_class.queues.length - 1);
            }
            for (Queue q : CP_class.queues) {
                logger.info("Adding checkpoint cmd of class {} to queue {}", CP_class, q.hashCode());
                q.add(cp);
            }
            starter++;
        }
    }

}
