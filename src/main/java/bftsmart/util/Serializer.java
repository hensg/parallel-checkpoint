/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.util;

import demo.bftmap.BFTMapRequestType;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.TreeMap;
import parallelism.EarlySchedulerMapping;
import parallelism.HibridClassToThreads;

/**
 *
 * @author juninho
 */
public class Serializer {
    
    public static void main(String args[]) throws FileNotFoundException, IOException, ClassNotFoundException{

        EarlySchedulerMapping e = new EarlySchedulerMapping();                                    
        int threads = 32;
        try{ 
            FileOutputStream fileOut =    
            new FileOutputStream(File.separator+"xabu"+File.separator+"mappingTo"+threads+"threads.ser");
            ObjectOutputStream out1 = new ObjectOutputStream(fileOut);
            out1.writeObject(e.generateMappings(threads));
            out1.close();
            fileOut.close();
        }catch(IOException ex){
            ex.printStackTrace();
        }
        FileInputStream file = new FileInputStream(File.separator+"xabu"+File.separator+"mappingTo"+threads+"threads.ser"); 
                ObjectInputStream in = new ObjectInputStream(file); 
                HibridClassToThreads[] h = (HibridClassToThreads[])in.readObject(); 
                for(int i=0;i<h[5].tIds.length;i++){
                    System.out.print(h[5].tIds[i]+" ");
                }
                
                        

    }
}
