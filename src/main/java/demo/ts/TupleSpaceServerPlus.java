/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package demo.ts;

import bftsmart.tom.ServiceReplica;
import parallelism.ParallelServiceReplica;
import parallelism.reconfiguration.LazyPolicy;

/**
 *
 * @author alchieri
 */
public class TupleSpaceServerPlus extends TupleSpaceServer {

    public TupleSpaceServerPlus(int id, int interval, int minT,int initT,int maxT, int entries, boolean context) {
        super(id, interval, minT, initT, maxT, entries, context);
    }

    
    protected void initReplica(int minT, int initT, int maxT, int id) {

        if (initT == 0) {
            System.out.println("Replica in sequential execution model.");
            replica = new ServiceReplica(id, this, null);
        } else {
            System.out.println("Replica in parallel execution model.");
           
            //replica = new ParallelServiceReplica(id, this, null, numThreads);
            /*replica = new ParallelServiceReplica(id, this, null, minT, initT, maxT, new LazyPolicy());
            
            
            for(int i = 0; i < initT;i++){
                for(int j = i+1; j < initT; j++){
                    int gid = (i+1)*10+(j+1);
                    int[] ids = new int[2];
                    ids[0]= i;
                    ids[1] = j;
                    
                    ((ParallelServiceReplica)replica).addExecutionConflictGroup(gid, ids);
                
                    System.out.println("Grupo +"+gid+" threads: "+ids[0]+" e "+ids[1]);
                }
            }
    
            */
            
        }

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

        new TupleSpaceServerPlus(processId, interval, mint,initt, maxt, entries, context);
    }

}
