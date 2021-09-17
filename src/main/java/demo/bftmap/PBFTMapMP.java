/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package demo.bftmap;



import bftsmart.tom.ParallelAsynchServiceProxy;
import bftsmart.tom.ParallelServiceProxy;
import bftsmart.tom.ServiceProxy;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.util.Storage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import parallelism.ParallelMapping;

/**
 *
 * @author alchieri
 */
public class PBFTMapMP implements Map<Integer, Map<Integer,byte[]>> {
    
  
    protected ParallelServiceProxy proxy = null;
    protected ByteArrayOutputStream out = null;
    protected boolean parallel = false;
    protected boolean async = false;
    protected ParallelAsynchServiceProxy asyncProxy = null;
    public Storage st = new Storage((1000000));
        public ArrayList<Long> values;
    public PBFTMapMP(int id, boolean parallelExecution, boolean async) {
        this.parallel = parallelExecution;
        this.async = async;
        this.values = new ArrayList();
        if(async){
            asyncProxy = new ParallelAsynchServiceProxy(id);
        }else{
             proxy = new ParallelServiceProxy(id);     
        }
    }
        @SuppressWarnings("unchecked")
	public Map<Integer,byte[]> get(Integer tableName) {
		try {
                        out = new ByteArrayOutputStream();
                        DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.GET);
			dos.writeInt(tableName);
                        byte[] rep;
                        if (parallel) {
                            if(async){
                                int id = asyncProxy.invokeParallelAsynchRequest(out.toByteArray(), null, TOMMessageType.ORDERED_REQUEST, ParallelMapping.CONC_ALL);
                                asyncProxy.cleanAsynchRequest(id);
                    
                                return null;
                            }else{
                                rep = proxy.invokeParallel(out.toByteArray(), ParallelMapping.CONC_ALL);
                            }
                        } else {
                                rep = proxy.invokeOrdered(out.toByteArray());
                        }
			//byte[] rep = KVProxy.invokeUnordered(out.toByteArray());
			ByteArrayInputStream bis = new ByteArrayInputStream(rep) ;
			ObjectInputStream in = new ObjectInputStream(bis) ;
			Map<Integer,byte[]> table = (Map<Integer,byte[]>) in.readObject();
			in.close();
			return table;
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(PBFTMapMP.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		} catch (IOException ex) {
			Logger.getLogger(PBFTMapMP.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}

	}
        
    	public byte[] getEntry(Integer tableName,Integer key) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.GET);
			dos.writeInt(tableName);
			dos.writeInt(key);
                        StringBuilder sb = new StringBuilder();
                        sb.append(tableName);
                        sb.append('#');
                        //dos.writeInt(new String(value));
                        byte[] rep=null;
                        if (parallel) {
                            if(async){
                                int id = asyncProxy.invokeParallelAsynchRequest(out.toByteArray(), null, TOMMessageType.ORDERED_REQUEST, ParallelMapping.SYNC_ALL);
                                asyncProxy.cleanAsynchRequest(id);
                    
                                return null;
                            }else{
                                long last = System.nanoTime();
                                rep = proxy.invokeParallel(out.toByteArray(), sb.toString().hashCode());
                                long now = System.nanoTime();
                                values.add(now-last);
                                
                            }
                        } else {
                                rep = proxy.invokeOrdered(out.toByteArray());
                        }
			return rep;
		} catch (IOException ex) {
			ex.printStackTrace();
			Logger.getLogger(PBFTMapMP.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}

	}
        public byte[] getEntries(Integer tableName1,Integer key1, Integer tableName2, Integer key2) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.GET12);
			dos.writeInt(tableName1);
			dos.writeInt(key1);
                        dos.writeInt(tableName2);
			dos.writeInt(key2);
                        StringBuilder sb = new StringBuilder();
                        sb.append(tableName1+'#'+tableName2+'#');
                        
                        
			//dos.writeUTF(new String(value));
                        byte[] rep;
                        if (parallel) {
                            if(async){
                                int id = asyncProxy.invokeParallelAsynchRequest(out.toByteArray(), null, TOMMessageType.ORDERED_REQUEST, ParallelMapping.SYNC_ALL);
                                asyncProxy.cleanAsynchRequest(id);
                    
                                return null;
                            }else{
                                    //System.out.println("parellel not async");  
                                    //rep = proxy.invokeParallel(out.toByteArray(), ParallelMapping.SYNC_ALL);
                                    if(tableName1==0){
                                        if(tableName2==1){
                                            String i = "01";
                                            long last = System.nanoTime();
                                            rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.R12);
                                            long now = System.nanoTime();
                                            ////System.out.println(now-last);
                                            values.add(now-last);
                                        }else if(tableName2==2){
                                            long last = System.nanoTime();
                                            rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.R13);
                                            long now = System.nanoTime();
                                            ////System.out.println(now-last);
                                            values.add(now-last);
                                        }else{
                                            long last = System.nanoTime();
                                            rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.R14);
                                            long now = System.nanoTime();
                                            ////System.out.println(now-last);
                                            values.add(now-last);
                                        }
                                        
                                    }else if (tableName1==1){
                                        if(tableName2==0){
                                            long last = System.nanoTime();
                                            rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.R12);
                                            long now = System.nanoTime();
                                            ////System.out.println(now-last);
                                            values.add(now-last);
                                        }else if(tableName2==2){
                                            long last = System.nanoTime();
                                            rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.R13);
                                            long now = System.nanoTime();
                                            ////System.out.println(now-last);
                                            values.add(now-last);
                                        }else{
                                            long last = System.nanoTime();
                                            rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.R14);
                                            long now = System.nanoTime();
                                            ////System.out.println(now-last);
                                            values.add(now-last);
                                        }
                                    }else if (tableName1==2){
                                        if(tableName2==0){
                                            long last = System.nanoTime();
                                            rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.R13);
                                            long now = System.nanoTime();
                                            //System.out.println(now-last);
                                            values.add(now-last);
                                        }else if(tableName2==1){
                                            long last = System.nanoTime();
                                            rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.R23);
                                            long now = System.nanoTime();
                                            //System.out.println(now-last);
                                            values.add(now-last);
                                        }else{
                                            long last = System.nanoTime();
                                            rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.R24);
                                            long now = System.nanoTime();
                                            //System.out.println(now-last);
                                            values.add(now-last);
                                        }
                                    }else{
                                        if(tableName2==0){
                                            long last = System.nanoTime();
                                            rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.R14);
                                            long now = System.nanoTime();
                                            //System.out.println(now-last);
                                            values.add(now-last);
                                        }else if(tableName2==1){
                                            long last = System.nanoTime();
                                            rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.R24);
                                            long now = System.nanoTime();
                                            ////System.out.println(now-last);
                                            values.add(now-last);
                                        }else{
                                            long last = System.nanoTime();
                                            rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.R34);
                                            long now = System.nanoTime();
                                            ////System.out.println(now-last);
                                            values.add(now-last);
                                        }
                                    }
                                
                            }
                        }else{
                                rep = proxy.invokeOrdered(out.toByteArray());
                        }
			return rep;
		} catch (IOException ex) {
			ex.printStackTrace();
			Logger.getLogger(PBFTMapMP.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}

	}
    @SuppressWarnings("unchecked")
	public Map<Integer,byte[]> put(Integer key, Map<Integer,byte[]> value) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.TAB_CREATE);
			dos.writeInt(key);
			//ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
			ObjectOutputStream  out1 = new ObjectOutputStream(out) ;
			out1.writeObject(value);
			out1.close();
			byte[] rep;
                        if (parallel) {
                            if(async){
                                int id = asyncProxy.invokeParallelAsynchRequest(out.toByteArray(), null, TOMMessageType.ORDERED_REQUEST, ParallelMapping.CONC_ALL);
                                asyncProxy.cleanAsynchRequest(id);
                    
                                return null;
                            }else{
                                //rep = proxy.invokeParallel(out.toByteArray(), ParallelMapping.CONC_ALL);
                                rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.GW);
         
                            }
                        } else {
                                rep = proxy.invokeOrdered(out.toByteArray());
                        }
			ByteArrayInputStream bis = new ByteArrayInputStream(rep) ;
			ObjectInputStream in = new ObjectInputStream(bis) ;
			Map<Integer,byte[]> table = null;
                    try {
                        table = (Map<Integer,byte[]>) in.readObject();
                    } catch (ClassNotFoundException ex) {
                        Logger.getLogger(PBFTMapMP.class.getName()).log(Level.SEVERE, null, ex);
                    }
			in.close();
			return table;

		} catch (IOException ex) {
			ex.printStackTrace();
			Logger.getLogger(PBFTMapMP.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}
	}
    public byte[] putEntry(Integer tableName, Integer key, byte[] value) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.PUT);
			dos.writeInt(tableName);
			dos.writeInt(key);
                        StringBuilder sb = new StringBuilder();
                        sb.append(tableName);
                        sb.append('#');
                        sb.append('S');
			dos.writeUTF(new String(value));
                        byte[] rep=null;
                        
                        if (parallel) {
                            if(async){
                                int id = asyncProxy.invokeParallelAsynchRequest(out.toByteArray(), null, TOMMessageType.ORDERED_REQUEST, ParallelMapping.SYNC_ALL);
                                asyncProxy.cleanAsynchRequest(id);
                    
                                return null;
                            }else{
                                    
                                        long last = System.nanoTime();
                                        rep = proxy.invokeParallel(out.toByteArray(), sb.toString().hashCode());
                                        //System.out.println(System.nanoTime()-last);
                                        long now = System.nanoTime();
                                        values.add(now-last);
                                
                            }
                        } else {
                                rep = proxy.invokeOrdered(out.toByteArray());
                        }
			return rep;
		} catch (IOException ex) {
			ex.printStackTrace();
			Logger.getLogger(PBFTMapMP.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}

	}

        public byte[] putEntries(Integer tableName1, Integer key1,Integer tableName2, Integer key2, byte[] value) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.PUT12);
			dos.writeInt(tableName1);
			dos.writeInt(key1);
                        dos.writeInt(tableName2);
                        StringBuilder sb = new StringBuilder();
                        sb.append(tableName1);
                        sb.append('#');
                        sb.append(tableName2);
                        sb.append('#');
                        //System.out.println("sb hash "+sb.toString().hashCode()+" string = "+sb.toString());
			dos.writeInt(key2);
			dos.writeUTF(new String(value));
                        byte[] rep;
                        if (parallel) {
                            if(async){
                                int id = asyncProxy.invokeParallelAsynchRequest(out.toByteArray(), null, TOMMessageType.ORDERED_REQUEST, ParallelMapping.SYNC_ALL);
                                asyncProxy.cleanAsynchRequest(id);
                    
                                return null;
                            }else{
                                long last = System.nanoTime();
                                rep = proxy.invokeParallel(out.toByteArray(), sb.toString().hashCode());         
                                long now = System.nanoTime();
                                values.add(now-last);
                            }
                        } else {
                                rep = proxy.invokeOrdered(out.toByteArray());
                        }
			return rep;
		} catch (IOException ex) {
			ex.printStackTrace();
			Logger.getLogger(PBFTMapMP.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}

	}    
    
    @SuppressWarnings("unchecked")
	public Map<Integer,byte[]> remove(Object key) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.TAB_REMOVE);
			dos.writeUTF((String) key);
			byte[] rep;
                        if (parallel) {
                            if(async){
                                int id = asyncProxy.invokeParallelAsynchRequest(out.toByteArray(), null, TOMMessageType.ORDERED_REQUEST, ParallelMapping.CONC_ALL);
                                asyncProxy.cleanAsynchRequest(id);
                    
                                return null;
                            }else{
                                rep = proxy.invokeParallel(out.toByteArray(), ParallelMapping.SYNC_ALL);
                            }
                        } else {
                                rep = proxy.invokeOrdered(out.toByteArray());
                        }
			ByteArrayInputStream bis = new ByteArrayInputStream(rep) ;
			ObjectInputStream in = new ObjectInputStream(bis) ;
			Map<Integer,byte[]> table = (Map<Integer,byte[]>) in.readObject();
			in.close();
			return table;
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(PBFTMapMP.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		} catch (IOException ex) {
			Logger.getLogger(PBFTMapMP.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}

	}
        public byte[] removeEntry(Integer tableName,Integer key)  {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.REMOVE);
			dos.writeInt( tableName);
			dos.writeInt(key);
			byte[] rep;
                        if (parallel) {
                            if(async){
                                int id = asyncProxy.invokeParallelAsynchRequest(out.toByteArray(), null, TOMMessageType.ORDERED_REQUEST, ParallelMapping.CONC_ALL);
                                asyncProxy.cleanAsynchRequest(id);
                    
                                return null;
                            }else{
                                rep = proxy.invokeParallel(out.toByteArray(), ParallelMapping.SYNC_ALL);
                            }
                        } else {
                                rep = proxy.invokeOrdered(out.toByteArray());
                        }

                        return rep;
		} catch (IOException ex) {
			Logger.getLogger(PBFTMapMP.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}

	}
   public long getPercentile(int percent){
      Collections.sort(values);
      int pos = (values.size()-1)*percent/100;
      return values.get(pos);
   }    
    public int size() {
		try {
			out = new ByteArrayOutputStream();
			new DataOutputStream(out).writeInt(BFTMapRequestType.SIZE_TABLE);
       			byte[] rep;
                        if (parallel) {
                            if(async){
                                int id = asyncProxy.invokeParallelAsynchRequest(out.toByteArray(), null, TOMMessageType.ORDERED_REQUEST, ParallelMapping.CONC_ALL);
                                asyncProxy.cleanAsynchRequest(id);
                    
                                return id;
                            }else{
                                rep = proxy.invokeParallel(out.toByteArray(), ParallelMapping.CONC_ALL);
                            }
                        } else {
                                rep = proxy.invokeOrdered(out.toByteArray());
                        }

                        ByteArrayInputStream in = new ByteArrayInputStream(rep);
			int size = new DataInputStream(in).readInt();
			return size;
		} catch (IOException ex) {
			Logger.getLogger(PBFTMapMP.class.getName()).log(Level.SEVERE, null, ex);
			return -1;
		}
	}

    public int size1(Integer tableName) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.SIZE);
			dos.writeInt(tableName);
			byte[] rep;
			if (parallel) {
                            if(async){
                                int id = asyncProxy.invokeParallelAsynchRequest(out.toByteArray(), null, TOMMessageType.ORDERED_REQUEST, ParallelMapping.CONC_ALL);
                                asyncProxy.cleanAsynchRequest(id);
                    
                                return id;
                            }else{
                                    if(tableName==0){
                                        //System.out.println("sending put to table0");
                                        //System.out.println("parallel+table0");
                                        //System.out.println("id = "+MultipartitionMapping.W1);
                                        long last = System.nanoTime();
                                        rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.W1);
                                        values.add(System.nanoTime()-last);
                                    }else if (tableName==1){
                                        //System.out.println("parallel+table1");
                                        //System.out.println("sending put to table1");
                                        long last = System.nanoTime();
                                        rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.W2);
                                        values.add(System.nanoTime()-last);
                                    }else if (tableName==2){
                                        //System.out.println("parallel+table2");
                                        //System.out.println("sending put to table2");
                                        long last = System.nanoTime();
                                        rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.W3);         
                                        values.add(System.nanoTime()-last);
                                    }else{
                                        //System.out.println("parallel+table3");
                                        //System.out.println("sending put to table3");
                                        long last = System.nanoTime();
                                        rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.W4);         
                                        values.add(System.nanoTime()-last);
                                    }
    
                            }
                        } else {
                                rep = proxy.invokeOrdered(out.toByteArray());
                        }

                        ByteArrayInputStream in = new ByteArrayInputStream(rep);
			int size = new DataInputStream(in).readInt();
			return size;
		} catch (IOException ex) {
			Logger.getLogger(PBFTMapMP.class.getName()).log(Level.SEVERE, null, ex);
			return 0;
		}
	}
    
    public boolean containsKey(Integer key) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.TAB_CREATE_CHECK);
			dos.writeInt(key);
			byte[] rep;
			if (parallel) {
                            if(async){
                                int id = asyncProxy.invokeParallelAsynchRequest(out.toByteArray(), null, TOMMessageType.ORDERED_REQUEST, ParallelMapping.CONC_ALL);
                                asyncProxy.cleanAsynchRequest(id);
                    
                                return false;
                            }else{
                                //inserir medicao de tempo aqui
                                //rep = proxy.invokeParallel(out.toByteArray(), ParallelMapping.CONC_ALL);
                                rep = proxy.invokeParallel(out.toByteArray(), MultipartitionMapping.GR);
                                //inserir medicaço de tempo aqui
                            }
                        } else {
                                rep = proxy.invokeOrdered(out.toByteArray());
                        }

                        ByteArrayInputStream in = new ByteArrayInputStream(rep);
			boolean res = new DataInputStream(in).readBoolean();
			return res;
		} catch (IOException ex) {
			ex.printStackTrace();
			Logger.getLogger(PBFTMapMP.class.getName()).log(Level.SEVERE, null, ex);
			return false;
		}

	}

    public boolean containsKey1(Integer tableName, Integer key) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.CHECK);
			dos.writeInt(tableName);
			dos.writeInt(key);
			byte[] rep;
						if (parallel) {
                            if(async){
                                int id = asyncProxy.invokeParallelAsynchRequest(out.toByteArray(), null, TOMMessageType.ORDERED_REQUEST, ParallelMapping.CONC_ALL);
                                asyncProxy.cleanAsynchRequest(id);
                    
                                return false;
                            }else{
                                rep = proxy.invokeParallel(out.toByteArray(), ParallelMapping.CONC_ALL);
                            }
                        } else {
                                rep = proxy.invokeOrdered(out.toByteArray());
                        }

                        ByteArrayInputStream in = new ByteArrayInputStream(rep);
			boolean res = new DataInputStream(in).readBoolean();
			return res;
		} catch (IOException ex) {
			Logger.getLogger(PBFTMapMP.class.getName()).log(Level.SEVERE, null, ex);
			return false;
		}
	}
    private long computeAverage(long[] values, boolean percent){
        java.util.Arrays.sort(values);
        int limit = 0;
        if(percent){
            limit = values.length/10;
        }
        long count = 0;
        for(int i = limit; i < values.length - limit;i++){
            System.out.println(values[i]/1000);
            count = count + values[i];
        }
        //System.out.println("count = "+count);
        return count/(values.length - 2*limit);
    }

    public boolean isEmpty() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

  
    
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    
    public Map<Integer, byte[]> get(Object key) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

   
    public void putAll(Map<? extends Integer, ? extends Map<Integer, byte[]>> m) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    
    public void clear() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    
    public Set<Integer> keySet() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    
    public Collection<Map<Integer, byte[]>> values() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    
    public Set<Entry<Integer, Map<Integer, byte[]>>> entrySet() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    
    public boolean containsKey(Object key) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    void insertValue(PBFTMapMP store, String tableName, int ops) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

        public void printValues(){
        for(int i=0;i<values.size();i++){
            System.out.println(values.get(i));
        }
    }
    
}