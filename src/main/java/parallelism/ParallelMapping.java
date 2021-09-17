/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;


/**
 *
 * @author alchieri
 */
public class ParallelMapping {
    
    
    public static final int CONC_ALL = -1;
    public static final int SYNC_ALL = -2;
    public static int CONFLICT_RECONFIGURATION = -3;

    private List<HibridClassToThreads> classes;
    
    private BlockingQueue[] queues;
    private CyclicBarrier reconfBarrier;


    
    public ParallelMapping(int numberOfThreads, HibridClassToThreads[] cToT) {
        
        queues = new BlockingQueue[numberOfThreads];
        System.out.println("queues length =="+queues.length);
        this.classes = new LinkedList<HibridClassToThreads>();
        for (int i = 0; i < queues.length; i++) {
            //queues[i] = new LinkedBlockingQueue();
            queues[i] = new FIFOQueue();
        }
        for(int i=0;i<cToT.length;i++)
            this.classes.add(cToT[i]);
        
        for(int i = 0; i < this.classes.size();i++){
            try{
                BlockingQueue[] q = new BlockingQueue[this.classes.get(i).tIds.length];
                for(int j = 0; j < q.length; j++){
                    if(this.classes.get(i).tIds[j]!=83)
                        q[j] = queues[this.classes.get(i).tIds[j]];

                }
                this.classes.get(i).setQueues(q);
            }catch(NullPointerException ex){
                System.out.println("error in mapping for i = "+i);
            }
        }
        

        //this.barriers.put(CONFLICT_ALL, new CyclicBarrier(getNumThreadsAC()));

        //this.executorThread.put(CONFLICT_ALL, 0);
        reconfBarrier = new CyclicBarrier(numberOfThreads);
        //reconfThreadBarrier = new CyclicBarrier(maxNumberOfthreads);
    }
    
    public int getNumWorkers(){
        return this.queues.length;
    }
    
    public HibridClassToThreads getClass(int id){
        for(int i = 0; i < this.classes.size(); i++){
            //System.out.println("CLASS ID = "+this.classes[i].classId);
            try{
                if(this.classes.get(i).classId == id){
                    return this.classes.get(i);
                }
                
            }catch(NullPointerException exp){
                System.out.println("error for i = "+this.classes.size());
            }    
        }
        return null;
    }

    public CyclicBarrier getBarrier(int classId) {
        //return barriers.get(groupID);
        return this.getClass(classId).barrier;
    }

    public int getExecutorThread(int classId) {
        return this.getClass(classId).tIds[0];
    }

    public CyclicBarrier getReconfBarrier() {
        return reconfBarrier;
    }
    
    public BlockingQueue[] getAllQueues() {
        return queues;
    }
    public HibridClassToThreads setQueue(HibridClassToThreads cls){
        
        BlockingQueue[] q = new BlockingQueue[cls.tIds.length];
                for(int j = 0; j < q.length; j++){
                    if(cls.tIds[j]!=83)
                        q[j] = queues[cls.tIds[j]];

                }
        cls.setQueues(q);
        this.classes.add(cls);
        return cls;
    }

}
