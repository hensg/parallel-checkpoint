/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.scheduler;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Queue;
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
public class DefaultScheduler implements Scheduler {

    private static final Logger logger = LoggerFactory.getLogger(DefaultScheduler.class);

    protected ParallelMapping mapping;
    private HashMap<Integer, HibridClassToThreads> classes;
    private int cmds;
    private int CPperiod;
    private int numWorkers;
    private HibridClassToThreads[] cts;

    public DefaultScheduler(int numberWorkers, int period) {
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
        this.classes = new HashMap<Integer, HibridClassToThreads>();
        for (int i = 0; i < cts.length; i++) {
            this.classes.put(cts[i].classId, cts[i]);
            logger.info("Classes with id {} to thread {} of type {}",
                    cts[i].classId, i,
                    this.classes.get(cts[i].classId).type);
        }
        this.mapping = new ParallelMapping(numberWorkers, cts);
        this.CPperiod = period;
        this.cmds = 0;
        this.numWorkers = numberWorkers;
    }

    @Override
    public int getNumWorkers() {
        return this.mapping.getNumWorkers();
    }

    @Override
    public HashMap<Integer, HibridClassToThreads> getClasses() {
        return this.classes;
    }

    @Override
    public ParallelMapping getMapping() {
        return mapping;
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
        // System.out.println("request id = "+request.classId);

        HibridClassToThreads ct = this.mapping.getClass(request.classId);
        if (ct == null) {
            // TRATAR COMO CONFLICT ALL
            // criar uma classe que sincroniza tudo
            logger.error("CLASStoTHREADs MAPPING NOT FOUND for classId {}", request.classId);
        }
        if (ct.type == ClassToThreads.CONC) {// conc
            ct.queues[ct.threadIndex].add(request);
            logger.debug("Added request to queue of thread {}", ct.threadIndex);
            ct.threadIndex = (ct.threadIndex + 1) % ct.queues.length;
        } else { // sync
            for (Queue q : ct.queues) {
                logger.debug("Added request to queue {}", q);
                q.add(request);
            }
        }
        cmds++;
        // se necessario checkpoint, cria request
        if (cmds % CPperiod == 0) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < numWorkers; i++) {
                sb.append(i);
                sb.append('#');
            }
            try {
                dos.writeInt(BFTMapRequestType.CKP);
                dos.writeUTF(sb.toString());
                dos.writeInt(request.request.getSequence());
            } catch (IOException ex) {
                logger.error("Failed to write CKP request to output bytestream", ex);
            }
            byte[] b = out.toByteArray();
            TOMMessage req = new TOMMessage(1, 1, request.m.getConsensusId() + 1, b, 1);
            req.groupId = sb.toString().hashCode();

            MessageContextPair cp = new MessageContextPair(req, req.groupId, 0, b, null);

            HibridClassToThreads CP_class = this.classes.get(cp.classId);
            if (CP_class == null) {
                int[] ids = new int[numWorkers];
                for (int i = 0; i < ids.length; i++) {
                    ids[i] = i;
                }
                CP_class = new HibridClassToThreads(sb.toString().hashCode(), HibridClassToThreads.SYNC, ids);
                this.mapping.setQueue(CP_class);
                this.classes.put(sb.toString().hashCode(), CP_class);
            }
            for (Queue q : CP_class.queues) {
                logger.info("Adding checkpoint cmd of class {} to queue {}", CP_class, q.hashCode());
                q.add(cp);
            }
        }
    }
}
