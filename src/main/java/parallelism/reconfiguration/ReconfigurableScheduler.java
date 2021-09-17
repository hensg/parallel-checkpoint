/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.reconfiguration;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.util.ThroughputStatistics;
import java.util.concurrent.BlockingQueue;

import parallelism.MessageContextPair;
import parallelism.ParallelMapping;

import parallelism.scheduler.DefaultScheduler;

/**
 *
 * @author eduardo
 */
public class ReconfigurableScheduler extends DefaultScheduler {

    protected PSMRReconfigurationPolicy reconf;

    //private ThroughputStatistics statistics;
    
    public ReconfigurableScheduler(int initialWorkers, int id){
         this(initialWorkers,initialWorkers,initialWorkers, null, id);
         
         
     }
    
    public ReconfigurableScheduler(int minWorkers, int initialWorkers, int maxWorkers, PSMRReconfigurationPolicy reconf, int id) {
        super(maxWorkers, 0);
        /*super(minWorkers, initialWorkers, maxWorkers);
        if(reconf == null){
            this.reconf = new DefaultPSMRReconfigurationPolicy();
        }else{
            this.reconf = reconf;
        }*/
        
        
        
    }
    
   
    
    
    
    
    @Override
    public void schedule(MessageContextPair request) {
        
        
       /* int ntReconfiguration = this.reconf.checkReconfiguration(request.classId, 
                this.mapping.getNumThreadsAC(), this.mapping.getNumMaxOfThreads());
        //examina se é possível reconfigurar ntReconfiguration threads
        ntReconfiguration = this.mapping.checkNumReconfigurationThreads(ntReconfiguration);


        if (ntReconfiguration !=0) {
            this.nextThread = 0;
            int nextN = mapping.getNumThreadsAC() + ntReconfiguration;
            mapping.setNumThreadsAC(nextN);
            //COLOCAR NA FILA DE TODAS AS THREADS UMA REQUEST DO TIPO THREADS_RECONFIGURATION
            TOMMessage reconf = new TOMMessage(nextN, 0, 0, 0, null, 0, TOMMessageType.ORDERED_REQUEST,
                    ParallelMapping.THREADS_RECONFIGURATION);
            MessageContextPair mRec = new MessageContextPair(reconf, ParallelMapping.THREADS_RECONFIGURATION, -1, null);
            BlockingQueue[] q = mapping.getQueues();
            try {
                for (BlockingQueue q1 : q) {
                    q1.put(mRec);
                }
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }

        } */

        super.schedule(request);
    }

}
