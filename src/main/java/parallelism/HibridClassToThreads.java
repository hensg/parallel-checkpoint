/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism;

import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.CyclicBarrier;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 *
 * @author eduardo
 */
public class HibridClassToThreads implements Serializable {

    private final static Logger logger = LoggerFactory.getLogger(HibridClassToThreads.class);
    public static int CONC = 0; // concurrent
    public static int SYNC = 1; // synchronized
    public int type;
    public int[] tIds;
    public int classId;

    public Queue<MessageContextPair>[] queues;

    public CyclicBarrier barrier;

    public int threadIndex = 0;

    public HibridClassToThreads(int classId, int type, int[] ids) {
        this.classId = classId;
        this.type = type;
        this.tIds = ids;
    }

    public void setQueues(Queue<MessageContextPair>[] q) {
        if (q.length != tIds.length) {
            logger.error("INCORRECT MAPPING");
            logger.error("q length = " + q.length + " tids length = " + tIds.length);
        }
        this.queues = q;
        if (this.type == SYNC) {
            this.barrier = new CyclicBarrier(tIds.length);
        }
    }

    public String toString() {
        String t = "CONC";
        if (type == SYNC) {
            t = "SYNC";
        }
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < tIds.length; j++) {
            // System.out.print(iv[j]);
            sb.append(tIds[j] + ",");
        }

        return "CtoT [type:" + t + ",classID:" + classId + ", threads:" + sb.toString() + "]";
    }

}
