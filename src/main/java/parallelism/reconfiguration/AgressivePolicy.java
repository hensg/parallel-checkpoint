/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism.reconfiguration;

/**
 *
 * @author alex
 */
public class AgressivePolicy implements PSMRReconfigurationPolicy {

    int conflict = 0;
    int nconflict = 0;

    // intervalo de verificação(amostra)
    private final int interval = 5000;

    @Override
    public int checkReconfiguration(int classId, int activeThreads, int numMaxThreads) {

        /*
         * if (classId == ParallelMapping.CONFLICT_ALL) { conflict++; } else if (classId
         * == ParallelMapping.CONFLICT_NONE) { nconflict++; }
         * 
         * if (conflict + nconflict == interval) {
         * 
         * int cp = (conflict * 100) / interval;
         * 
         * 
         * this.conflict = 0; this.nconflict = 0;
         * 
         * if (cp < 20) { //FULL POWER return numMaxThreads - activeThreads; } else if
         * (activeThreads == 1) { return 0; } else { return (activeThreads - 1) * (-1);
         * } }
         */

        return 0;
    }

}
