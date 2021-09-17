/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.demo.bftmap;

import bftsmart.tom.ParallelAsynchServiceProxy;
import bftsmart.tom.ParallelServiceProxy;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.TreeMap;
import java.util.Map;

import bftsmart.tom.ServiceProxy;
import bftsmart.tom.core.messages.TOMMessageType;
import parallelism.ParallelMapping;

/**
 * Map implementation backed by a BFT replicated table.
 * 
 * @author sweta
 */
public class BFTMap implements Map<String, Map<String,byte[]>> {

    ServiceProxy KVProxy = null; // original do bftsmart
    protected ParallelServiceProxy proxy = null; // alchieri
    protected boolean parallel = false;
    protected boolean async = false;
    ByteArrayOutputStream out = null;
    protected ParallelAsynchServiceProxy asyncProxy = null; // alchieri
    
	public BFTMap(int id) {
		KVProxy = new ServiceProxy(id, "config");
	}
	public BFTMap(int id, boolean parallelExecution, boolean async){
                   
        this.parallel = parallelExecution;
        this.async = async;
        if(async){
            asyncProxy = new ParallelAsynchServiceProxy(id);
        }else{
             proxy = new ParallelServiceProxy(id);     
        }
        }
	@SuppressWarnings("unchecked")
	public Map<String,byte[]> get(String tableName) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.GET);
			dos.writeUTF(tableName);
                        byte[] rep = null;    
			//byte[] rep = KVProxy.invokeUnordered(out.toByteArray());
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
                        ByteArrayInputStream bis = new ByteArrayInputStream(rep) ;
			ObjectInputStream in = new ObjectInputStream(bis) ;
			Map<String,byte[]> table = (Map<String,byte[]>) in.readObject();
			in.close();
			return table;
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(BFTMap.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		} catch (IOException ex) {
			Logger.getLogger(BFTMap.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}

	}

	public byte[] getEntry(String tableName,String key) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.GET);
			dos.writeUTF(tableName);
			dos.writeUTF(key);
			//byte[] rep = KVProxy.invokeUnordered(out.toByteArray());
                                    byte[] rep = null;
            if (parallel) {
                if(async){
                    int id = asyncProxy.invokeParallelAsynchRequest(out.toByteArray(), null, TOMMessageType.ORDERED_REQUEST, ParallelMapping.SYNC_ALL);
                    asyncProxy.cleanAsynchRequest(id);   
                    return rep;
                }else{
                    rep = proxy.invokeParallel(out.toByteArray(), ParallelMapping.SYNC_ALL);
                }
            } else {
                rep = proxy.invokeOrdered(out.toByteArray());
            }
			return rep;
		} catch (IOException ex) {
			Logger.getLogger(BFTMap.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public Map<String,byte[]> put(String key, Map<String,byte[]> value) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.TAB_CREATE);
			dos.writeUTF(key);
			//ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
			ObjectOutputStream  out1 = new ObjectOutputStream(out) ;
			out1.writeObject(value);
			out1.close();
			//byte[] rep = KVProxy.invokeOrdered(out.toByteArray());
                        byte[] rep = null;
                        if (parallel) {
                if(async){
                    
                    
                    System.out.println("PT AT out.size()="+out.toByteArray().length);
                    int id = asyncProxy.invokeParallelAsynchRequest(out.toByteArray(), null, TOMMessageType.ORDERED_REQUEST, ParallelMapping.SYNC_ALL);
                    asyncProxy.cleanAsynchRequest(id);
                    return null;
                }else{
                    System.out.println("PT AF out.size()="+out.toByteArray().length);
                    rep = proxy.invokeParallel(out.toByteArray(), ParallelMapping.SYNC_ALL);
                }
            } else {
                System.out.println("PF out.size()="+out.toByteArray().length);
                rep = proxy.invokeOrdered(out.toByteArray());
                System.out.println("Passou de invokeOrdered(out.toByteArray)");
            }
			ByteArrayInputStream bis = new ByteArrayInputStream(rep) ;
			ObjectInputStream in = new ObjectInputStream(bis) ;
			Map<String,byte[]> table = (Map<String,byte[]>) in.readObject();
			in.close();
			return table;

		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
			Logger.getLogger(BFTMap.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		} catch (IOException ex) {
			ex.printStackTrace();
			Logger.getLogger(BFTMap.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}
	}

	public byte[] putEntry(String tableName, String key, byte[] value) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.PUT);
			dos.writeUTF(tableName);
			dos.writeUTF(key);
			dos.writeUTF(new String(value));
			byte[] rep = KVProxy.invokeOrdered(out.toByteArray());
			return rep;
		} catch (IOException ex) {
			ex.printStackTrace();
			Logger.getLogger(BFTMap.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}

	}

	@SuppressWarnings("unchecked")
	public Map<String,byte[]> remove(Object key) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.TAB_REMOVE);
			dos.writeUTF((String) key);
			byte[] rep = KVProxy.invokeOrdered(out.toByteArray());

			ByteArrayInputStream bis = new ByteArrayInputStream(rep) ;
			ObjectInputStream in = new ObjectInputStream(bis) ;
			Map<String,byte[]> table = (Map<String,byte[]>) in.readObject();
			in.close();
			return table;
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(BFTMap.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		} catch (IOException ex) {
			Logger.getLogger(BFTMap.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}

	}

	public byte[] removeEntry(String tableName,String key)  {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.REMOVE);
			dos.writeUTF((String) tableName);
			dos.writeUTF((String) key);
			byte[] rep = KVProxy.invokeOrdered(out.toByteArray());
			return rep;
		} catch (IOException ex) {
			Logger.getLogger(BFTMap.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}

	}
	public int size() {
		try {
			out = new ByteArrayOutputStream();
			new DataOutputStream(out).writeInt(BFTMapRequestType.SIZE_TABLE);
			byte[] rep;
			rep = KVProxy.invokeUnordered(out.toByteArray());
			ByteArrayInputStream in = new ByteArrayInputStream(rep);
			int size = new DataInputStream(in).readInt();
			return size;
		} catch (IOException ex) {
			Logger.getLogger(BFTMap.class.getName()).log(Level.SEVERE, null, ex);
			return -1;
		}
	}

	public int size1(String tableName) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.SIZE);
			dos.writeUTF(tableName);
			byte[] rep;
			rep = KVProxy.invokeUnordered(out.toByteArray());
			ByteArrayInputStream in = new ByteArrayInputStream(rep);
			int size = new DataInputStream(in).readInt();
			return size;
		} catch (IOException ex) {
			Logger.getLogger(BFTMap.class.getName()).log(Level.SEVERE, null, ex);
			return 0;
		}
	}

	public boolean containsKey(String key) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.TAB_CREATE_CHECK);
			dos.writeUTF((String) key);
			byte[] rep;
			rep = KVProxy.invokeUnordered(out.toByteArray());
			ByteArrayInputStream in = new ByteArrayInputStream(rep);
			boolean res = new DataInputStream(in).readBoolean();
			return res;
		} catch (IOException ex) {
			ex.printStackTrace();
			Logger.getLogger(BFTMap.class.getName()).log(Level.SEVERE, null, ex);
			return false;
		}

	}

	public boolean containsKey1(String tableName, String key) {
		try {
			out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out); 
			dos.writeInt(BFTMapRequestType.CHECK);
			dos.writeUTF((String) tableName);
			dos.writeUTF((String) key);
			byte[] rep;
			rep = KVProxy.invokeUnordered(out.toByteArray());
			ByteArrayInputStream in = new ByteArrayInputStream(rep);
			boolean res = new DataInputStream(in).readBoolean();
			return res;
		} catch (IOException ex) {
			Logger.getLogger(BFTMap.class.getName()).log(Level.SEVERE, null, ex);
			return false;
		}
	}




	public boolean isEmpty() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void putAll(Map m) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void clear() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public Set keySet() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public Collection values() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public Set entrySet() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public boolean containsKey(Object key) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public TreeMap<String, byte[]> get(Object key) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

}