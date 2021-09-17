/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package demo.ts;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

/**
 *
 * @author alchieri
 */
public class WorkloadGeneratorTS {

    private int percent;
    private int[] operations;

    public WorkloadGeneratorTS(int percent, int size) {
        this.percent = percent;
        this.operations = new int[size];
        generate();
    }

    public int[] getOperations() {
        return operations;
    }

    private void generate() {
        String sep = System.getProperty("file.separator");
        String path = "config" + sep + "workloadP_BFT_SMART";
        File f = new File(path);
        if (!f.exists()) {
            f.mkdirs();
        }

        path = path + sep + "workload_ts" + operations.length + ".txt";

        f = new File(path);
        if (f.exists()) {
            load(path);
        } else {

            try {
                FileWriter fw = new FileWriter(f);
                PrintWriter pw = new PrintWriter(fw);

                Random rand = new Random();
                int op = 0;
                int num = 0;
                
                int out = 0;
                int rdp = 0;
                int inp = 0;
                int cas = 0;
                
                while (num < this.operations.length) {

                    op = rand.nextInt(4)+1;
                    //System.out.println("Sorteou: "+op);
                    if (op == BFTTupleSpace.OUT) {
                        
                        if(out == 0 || ((double) operations.length/out) > 4){
                           out++;
                           pw.println(op);
                           this.operations[num] = op;
                            num++;     
                        }
                    }else if (op == BFTTupleSpace.RDP) {
                        
                        if(rdp == 0 || ((double)operations.length/rdp) > 4){
                           rdp++;
                           pw.println(op);
                           this.operations[num] = op;
                           num++;     
                        }
                    }else if (op == BFTTupleSpace.INP) {
                        
                        if(inp == 0 || ((double)operations.length/inp) > 4){
                           inp++;
                           pw.println(op);
                           this.operations[num] = op;
                            num++;     
                        }
                    }else {
                        
                        if(cas == 0 || ((double)operations.length/cas) > 4){
                           cas++;
                           pw.println(op);
                           this.operations[num] = op;
                            num++;     
                        }
                    }
                }
                pw.flush();
                fw.flush();
                pw.close();
                fw.close();
                
                System.out.println("OUT: "+out);
                System.out.println("RDP: "+rdp);
                System.out.println("INP: "+inp);
                System.out.println("CAS: "+cas);
                
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

        }
    }
    
    private void generate_old() {
        String sep = System.getProperty("file.separator");
        String path = "config" + sep + "workloadP_BFT_SMART";
        File f = new File(path);
        if (!f.exists()) {
            f.mkdirs();
        }

        path = path + sep + "workload_ts" + percent + ".txt";

        f = new File(path);
        if (f.exists()) {
            load(path);
        } else {

            try {
                FileWriter fw = new FileWriter(f);
                PrintWriter pw = new PrintWriter(fw);

                Random rand = new Random();
                int op = 0;
                int num = 0;
                int cnf = 0;
                int ncnf = 0;
                
                int ncnfT = ((100-this.percent) * this.operations.length)/100;
                int cnfT = (this.percent * this.operations.length)/100;
                
                while (num < this.operations.length) {

                    int r = rand.nextInt(100);
                    if ((cnf == cnfT) || (r >= percent && ncnf < ncnfT)) {
                        ncnf++;
                        //nao conflitantes
                        op = BFTTupleSpace.RDP;
                            
                       
                    } else {
                        cnf++;
                        //conflitante
                        r = rand.nextInt(2);
                        if (r >= 1) {
                            //OUT
                            op = BFTTupleSpace.OUT;
                        } else {
                            //INP
                            op = BFTTupleSpace.INP;
                        }
                    }
                    
                    
                    pw.println(op);
                    this.operations[num] = op;
                    num++;
                    
                }

                pw.flush();
                fw.flush();
                pw.close();
                fw.close();
                
                System.out.println("Conflitantes: "+cnf);
                System.out.println("Não Conflitantes: "+ncnf);
                
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

        }
    }

    private void load(String path) {
        //System.out.println("Vai ler!!!");
        try {

            FileReader fr = new FileReader(path);

            BufferedReader rd = new BufferedReader(fr);
            String line = null;
            int j = 0;
            while (((line = rd.readLine()) != null) && (j < operations.length)) {
                operations[j] = Integer.valueOf(line);
                //System.out.println("Leu:" + operations[j]);
                j++;
            }
            fr.close();
            rd.close();
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }

    public static void main(String[] args) {
        new WorkloadGeneratorTS(0, 1000);
        
        /*new WorkloadGeneratorTS(25, 1000);
        
        new WorkloadGeneratorTS(50, 1000);
        
        new WorkloadGeneratorTS(75, 1000);
        
        new WorkloadGeneratorTS(100, 1000);*/
    }

}
