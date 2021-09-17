package parallelism;

import bftsmart.consensus.messages.MessageFactory;
import bftsmart.consensus.roles.Acceptor;
import bftsmart.consensus.roles.Proposer;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ParallelSingleExecutable;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.ExecutionManager;
import bftsmart.tom.core.ParallelTOMLayer;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.server.Executable;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import bftsmart.tom.util.ShutdownHookThread;
import bftsmart.tom.util.TOMUtil;
import bftsmart.util.MultiOperationRequest;
import bftsmart.util.Pair;
import bftsmart.util.ThroughputStatistics;
import demo.bftmap.BFTMapRequestType;
import demo.bftmap.MultipartitionMapping;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.nio.file.Path;
import javax.xml.transform.Source;
import parallelism.reconfiguration.PSMRReconfigurationPolicy;
import parallelism.reconfiguration.ReconfigurableScheduler;
import parallelism.scheduler.DefaultScheduler;
import parallelism.scheduler.ParallelScheduler;
import parallelism.scheduler.Scheduler;

public class ParallelServiceReplica extends ServiceReplica {
    public Receiver[] receivers;
    int num_partition;
    int error=0;
    public boolean recovering = true;
    public boolean partition;
    protected Scheduler scheduler;
    public ThroughputStatistics statistics;
    public ServiceReplicaWorker[] workers;
    protected Map<String, MultiOperationCtx> ctxs = new Hashtable<>();
    public RecoverThread[] recoverers;
    public String[] paths;
    public int scheduled =0;
    public ParallelServiceReplica(int id, Executable executor, Recoverable recoverer, int initialWorkers,int period,boolean part) throws IOException, ClassNotFoundException {
        super(id, executor, null);
        
        this.partition =part;
        this.paths = new String[initialWorkers];
        for(int i=0;i<this.paths.length;i++){
            this.paths[i]=Integer.toString(i%5);
        }
        
        this.num_partition=initialWorkers;
        if (initialWorkers <= 0) {
            initialWorkers = 1;
        }
        
        if(partition){
            System.out.println("MULTI PARTITION WITH "+initialWorkers+" THREADS");
            this.scheduler = new ParallelScheduler(this.id,initialWorkers,period);
        }else{
            System.out.println("SINGLE PARTITION WITH "+initialWorkers+" THREADS");
            this.scheduler = new DefaultScheduler(initialWorkers, period);
        }
            
        initWorkers(this.scheduler.getNumWorkers(), id);

        
        if(this.id==2){
            System.out.println("RECOVERING");
            //recover();
           // this.tomLayer.getDeliveryThread().deliverLock();
            if(partition){
                this.receivers = new Receiver[initialWorkers];

                for(int rec=0;rec<initialWorkers;rec++){
                    this.receivers[rec]=new Receiver(rec);
                    this.receivers[rec].start(); 
                }
            
            }else{
                    this.receivers = new Receiver[1];

                    int rec=0;
                    this.receivers[rec]=new Receiver(rec);
                    this.receivers[rec].start(); 
                
            
            
            }
            //this.tomLayer.getDeliveryThread().deliverUnlock();
//                this.receivers[0]=new Receiver(0);
//                this.receivers[0].start();
        }else{
            this.recoverers = new RecoverThread[initialWorkers];
            for(int i=0;i<initialWorkers;i++){
                try {
                    recoverers[i] = new RecoverThread(i);
                } catch (IOException ex) {
                    Logger.getLogger(ParallelServiceReplica.class.getName()).log(Level.SEVERE, null, ex);
                }
                recoverers[i].start();
            }
            this.recovering=false;
        }
        
    }
    
    
    protected void initWorkers(int n, int id) {
        this.workers = new ServiceReplicaWorker[n];
        statistics = new ThroughputStatistics(n, "results_" + id + ".txt", "");
        statistics.start();
        int tid = 0;
        System.out.println("here?");
        for (int i = 0; i < n; i++) {
            System.out.println("i = "+i);
            workers[i] = new ServiceReplicaWorker((FIFOQueue) this.scheduler.getMapping().getAllQueues()[i], tid);
            workers[i].start();
            tid++;
        }
    }

    public int getNumActiveThreads() {
        return this.scheduler.getMapping().getNumWorkers();
    }
    public CyclicBarrier getReconfBarrier() {
        return this.scheduler.getMapping().getReconfBarrier();
    }

    public void receiveMessages(int consId[], int regencies[], int leaders[], CertifiedDecision[] cDecs, TOMMessage[][] requests) {
                        
        Iterator<String> it = ctxs.keySet().iterator(); 
        while (it.hasNext()) {
            String next = it.next();
            MultiOperationCtx cx = ctxs.get(next);

            if (cx.finished) {
                it.remove();
            }

        }
        
        int consensusCount = 0;
        boolean noop = true;
        //System.out.println("Received message");
        for (TOMMessage[] requestsFromConsensus : requests) {
            TOMMessage firstRequest = requestsFromConsensus[0];
            int requestCount = 0;
            noop = true;
            for (TOMMessage request : requestsFromConsensus) {

                bftsmart.tom.util.Logger.println("(ServiceReplica.receiveMessages) Processing TOMMessage from client " + request.getSender() + " with sequence number " + request.getSequence() + " for session " + request.getSession() + " decided in consensus " + consId[consensusCount]);
                //System.out.println("received message "+request.getId()); 
        
                if (request.getViewID() == SVController.getCurrentViewId()) {
                    if (request.getReqType() == TOMMessageType.ORDERED_REQUEST) {
                        noop = false;
                        //numRequests++;
                        

                        request.deliveryTime = System.nanoTime();
                        MessageContextPair msg=null;
                        if(recovering){
                             MultiOperationRequest reqs = new MultiOperationRequest(request.getContent());
                             msg = new MessageContextPair(request, request.groupId, 0, reqs.operations[0].data,null);
             
                        }else{ 
                            MultiOperationRequest reqs = new MultiOperationRequest(request.getContent());
                            MultiOperationCtx ctx = new MultiOperationCtx(reqs.operations.length, request);  
                            this.ctxs.put(request.toString(), ctx);
                            MessageContext m = new MessageContext(request.getSender(), request.getViewID(),
                                    request.getReqType(), request.getSession(), request.getSequence(), request.getOperationId(),
                                    request.getReplyServer(), request.serializedMessageSignature, firstRequest.timestamp,
                                    request.numOfNonces, request.seed, regencies[consensusCount], leaders[consensusCount],
                                    consId[consensusCount], cDecs[consensusCount].getConsMessages(), firstRequest, true);
                                 m.setLastInBatch();     
                                 msg = new MessageContextPair(request, request.groupId, 0, reqs.operations[0].data,m);
                        }     
                             
                            
                             //if(msg.request.getSequence()>this.tomLayer.getLastExec() && this.tomLayer.getLastExec()!=0)   
                                this.scheduler.schedule(msg);
                                //System.out.println("scheduled");
//                             this.scheduled++;
//                             if(scheduled>=200000 && this.id==0){
//                                 System.out.println("RESTARTING REPLICA");
//                                 this.restart();
//                                 
//                             }
//                                 
                           //implementar log de requests 

                    }else if (request.getReqType() == TOMMessageType.RECONFIG) {

                        SVController.enqueueUpdate(request);
                    }else{
                        throw new RuntimeException("Should never reach here! ");
                    }
                }else if (request.getViewID() < SVController.getCurrentViewId()) {
                    // message sender had an old view, resend the message to
                    // him (but only if it came from consensus an not state transfer)
                    tomLayer.getCommunication().send(new int[]{request.getSender()}, new TOMMessage(SVController.getStaticConf().getProcessId(),
                            request.getSession(), request.getSequence(), TOMUtil.getBytes(SVController.getCurrentView()), SVController.getCurrentViewId()));
                    
                }
                
                requestCount++;
            }
            consensusCount++;
            //System.out.println("globalC = "+globalC);
        }
        if (SVController.hasUpdates()) {

            this.scheduler.scheduleReplicaReconfiguration();

        }
    }
    
    
    private void initTOMLayer() {
        if (tomStackCreated) { // if this object was already initialized, don't do it again
            return;
        }

        if (!SVController.isInCurrentView()) {
            throw new RuntimeException("I'm not an acceptor!");
        }

        
        MessageFactory messageFactory = new MessageFactory(id);

        Acceptor acceptor = new Acceptor(cs, messageFactory, SVController);
        cs.setAcceptor(acceptor);

        Proposer proposer = new Proposer(cs, messageFactory, SVController);

        ExecutionManager executionManager = new ExecutionManager(SVController, acceptor, proposer, id);

        acceptor.setExecutionManager(executionManager);

        tomLayer = new ParallelTOMLayer(executionManager, this, recoverer, acceptor, cs, SVController, verifier);

        executionManager.setTOMLayer(tomLayer);

        SVController.setTomLayer(tomLayer);

        cs.setTOMLayer(tomLayer);
        cs.setRequestReceiver(tomLayer);

        acceptor.setTOMLayer(tomLayer);

        if (SVController.getStaticConf().isShutdownHookEnabled()) {
            Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(tomLayer));
        }
        tomLayer.start(); // start the layer execution
        tomStackCreated = true;

        replicaCtx = new ReplicaContext(cs, SVController);
    }


    
    private class Checkpointer extends Thread{
        private int cp_id;
        public boolean doCP;
        //public byte[] req = null;
        public MessageContextPair req = null;
        
        //public Queue<byte[]> requests;
        public Queue<MessageContextPair> requests;
        
        public Checkpointer(int id){
            this.cp_id = id;
            this.requests = new LinkedList<MessageContextPair>();
            this.doCP = false;
        }
            public void run(){
                //System.out.println("checkpointer "+id+" starting.");
                while(true){      
                    synchronized(this){
                        try{
                            this.wait();
                        }catch(InterruptedException e){
                            e.printStackTrace();
                        }
                        while (this.requests.size()>0) {
                            req = this.requests.poll();
                            byte[] b;
                            int cid=0;
                            String filename;
                            String dataname;
                            if(partition){
                                ByteArrayInputStream in = new ByteArrayInputStream(req.request.getContent());
                                
                                try {
                                    int n = new DataInputStream(in).readInt();
                                    String s = new DataInputStream(in).readUTF();
                                    cid = new DataInputStream(in).readInt();

                                } catch (IOException ex) {
                                    Logger.getLogger(ParallelServiceReplica.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                ByteArrayOutputStream out = new ByteArrayOutputStream();
                                DataOutputStream dos = new DataOutputStream(out); 
                                String s = Integer.toString(this.cp_id);
                                try {
                                    dos.writeInt(BFTMapRequestType.CKP);
                                    dos.writeUTF(s);
                                } catch (IOException ex) {
                                    Logger.getLogger(ParallelScheduler.class.getName()).log(Level.SEVERE, null, ex);
                                }


                                b =((SingleExecutable) executor).executeOrdered(out.toByteArray(),req.m);
                                filename = "map"+this.cp_id+".ser";
                                dataname = "map"+this.cp_id+"-metadata.txt";
                                //System.out.println("array size = "+req.length);
                                                                               
                            try{ 
                                FileOutputStream fileOut =    
                                //new FileOutputStream(File.separator+"recovery"+File.separator+"states"+File.separator+filename);
                                new FileOutputStream(File.separator+"checkpoint"+paths[cp_id]+File.separator+"states"+File.separator+filename);
                                
                                ObjectOutputStream out1 = new ObjectOutputStream(fileOut);
                                out1.writeObject(b);
                                out1.close();
                                fileOut.close();
                                BufferedWriter writer = 
                                        //new BufferedWriter(new FileWriter(File.separator+"recovery"+File.separator+"metadata"+File.separator+dataname));
                                        new BufferedWriter(new FileWriter(File.separator+"checkpoint"+paths[cp_id]+File.separator+"metadata"+File.separator+dataname));
                                
                                writer.write(Integer.toString(cid));
                                writer.write("\n#");
                                writer.close();
                                this.notify();
                            }catch(Exception e){
                                e.printStackTrace();
                            }

                            }else{
                                b =((SingleExecutable) executor).executeOrdered(req.request.getContent(),req.m);
                                filename = "ALLmap"+this.cp_id+".ser";
                                dataname = "ALLmap-metadata.txt";
                                                                               
                            try{ 
                                FileOutputStream fileOut =    
                                //new FileOutputStream(File.separator+"recovery"+File.separator+"states"+File.separator+filename);
                                new FileOutputStream(File.separator+"checkpoint"+paths[cp_id]+File.separator+"states"+File.separator+filename);
                                
                                ObjectOutputStream out1 = new ObjectOutputStream(fileOut);
                                out1.writeObject(b);
                                out1.close();
                                fileOut.close();
                                BufferedWriter writer = 
                                        //new BufferedWriter(new FileWriter(File.separator+"recovery"+File.separator+"metadata"+File.separator+dataname));
                                        new BufferedWriter(new FileWriter(File.separator+"checkpoint0"+File.separator+"metadata"+File.separator+dataname));
                                
                                writer.write(Integer.toString(cid));
                                writer.write("\n#");
                                writer.close();
                                this.notify();
                            }catch(Exception e){
                                e.printStackTrace();
                            }
                            }

                        }
                        //System.out.println("thread "+id+" done checkpoint");
                        
                    }
                }
            }
            public void addRequest(MessageContextPair m){
                //System.out.println(this.id+" added to queue");
                this.requests.add(m);
                
            }    
    }
    private class ServiceReplicaWorker extends Thread {

        private FIFOQueue<MessageContextPair> requests;
        private int thread_id;
        private Checkpointer checkpointer;
        private FIFOQueue<Integer> interacoes = new FIFOQueue<>();
        public Queue<Operation> log;
        public ServiceReplicaWorker(FIFOQueue<MessageContextPair> requests, int id) {
            this.log = new LinkedList<Operation>();
            this.thread_id = id;
            this.requests = requests;
            this.checkpointer = new Checkpointer(this.thread_id);
            this.checkpointer.start();
            //System.out.println("new");
        }
        int localC = 0;
        int localTotal = 0;
        
        public void run() {
            MessageContextPair msg = null;
            ExecutionFIFOQueue<MessageContextPair> execQueue = new ExecutionFIFOQueue();            
            while (true) {
                
                try {
                    this.requests.drainToQueue(execQueue);
                    localC++;
                    localTotal = localTotal + execQueue.getSize();
                   
                    do {
                            
                        msg = execQueue.getNext();
                        
                        HibridClassToThreads ct = scheduler.getClasses().get(msg.classId);
                        
                        ByteArrayInputStream in = new ByteArrayInputStream(msg.request.getContent());
                        int cmd = new DataInputStream(in).readInt();
                        //System.out.println("cmd = "+cmd);    
                        if (ct.type == ClassToThreads.CONC){ 
                            
                                msg.resp = ((SingleExecutable) executor).executeOrdered(msg.request.getContent(),msg.m);                                
                                if(!recovering){
                                MultiOperationCtx ctx = ctxs.get(msg.request.toString());
                                    
                                    
                                    this.log.add(new Operation(cmd, msg.classId, msg.request.getContent(),msg.request.getSequence()));
                                    //System.out.println("log peek = "+log.peek().classID);
                                
                                    ctx.add(msg.index, msg.resp);                                
                                    if (ctx.response.isComplete() && !ctx.finished && (ctx.interger.getAndIncrement() == 0)) {
                                        ctx.finished = true;
                                        ctx.request.reply = new TOMMessage(id, ctx.request.getSession(),
                                        ctx.request.getSequence(), msg.resp, SVController.getCurrentViewId());
                                        replier.manageReply(ctx.request, msg.m);

                                    }
                                    statistics.computeStatistics(thread_id, 1);
                                }
                                
                        } else if ((ct.type == ClassToThreads.SYNC && ct.tIds.length == 1)) {//SYNC mas só com 1 thread, não precisa usar barreira
                            
                            if(cmd<BFTMapRequestType.CKP && cmd>0){
                                msg.resp = ((SingleExecutable) executor).executeOrdered(msg.request.getContent(), msg.m);
                                this.log.add(new Operation(cmd, msg.classId, msg.request.getContent(),msg.request.getSequence()));
                                //System.out.println("log peek = "+log.peek());
                                MultiOperationCtx ctx = ctxs.get(msg.request.toString());
                                //System.out.println("CMD = "+cmd);
                                ctx.add(msg.index, msg.resp);
                                if (ctx.response.isComplete() && !ctx.finished && (ctx.interger.getAndIncrement() == 0)) {
                                    ctx.finished = true;
                                    ctx.request.reply = new TOMMessage(id, ctx.request.getSession(),
                                    ctx.request.getSequence(), msg.resp, SVController.getCurrentViewId());
                                    replier.manageReply(ctx.request, msg.m);        
                                }
                                statistics.computeStatistics(thread_id, 1);
                            }else if (cmd==BFTMapRequestType.CKP){
                                synchronized(this.checkpointer){
                                        checkpointer.addRequest(msg);
                                        this.checkpointer.notify();
                                        this.checkpointer.wait();
                                        this.log.clear();
                                }
                                

                            }
                            
                        } else if (ct.type == ClassToThreads.SYNC ){
                            if (thread_id == scheduler.getMapping().getExecutorThread(msg.classId)) {
                                
                                scheduler.getMapping().getBarrier(msg.classId).await();
                                if(cmd==BFTMapRequestType.CKP){
                                    
                                    synchronized(this.checkpointer){
                                        checkpointer.addRequest(msg);
                                        this.checkpointer.notify();                                
                                        this.checkpointer.wait();
                                        this.log.clear();                                        
                                    }
                                    
                                }else{
                                    msg.resp = ((SingleExecutable) executor).executeOrdered(msg.request.getContent(), msg.m);              
                                    this.log.add(new Operation(cmd, msg.classId, msg.request.getContent(),msg.request.getSequence()));
                                    MultiOperationCtx ctx = ctxs.get(msg.request.toString());
                                    ctx.add(msg.index, msg.resp);
                                    if (ctx.response.isComplete() && !ctx.finished && (ctx.interger.getAndIncrement() == 0)) {
                                        ctx.finished = true;
                                        ctx.request.reply = new TOMMessage(id, ctx.request.getSession(),
                                        ctx.request.getSequence(), msg.resp, SVController.getCurrentViewId());
                                        replier.manageReply(ctx.request, msg.m);
                                                                 
                                    }
                                }
                                statistics.computeStatistics(thread_id, 1);
                                scheduler.getMapping().getBarrier(msg.classId).await();
                            } else{                                
                                scheduler.getMapping().getBarrier(msg.classId).await();
                                    if(cmd==BFTMapRequestType.CKP && partition){
                                        synchronized(this.checkpointer){                                            
                                            checkpointer.addRequest(msg);
                                            this.checkpointer.notify();
                                            this.checkpointer.wait();
                                            this.log.clear();
                                        }
                                    }
                                scheduler.getMapping().getBarrier(msg.classId).await();
                            }

                        } else if (msg.classId == ParallelMapping.CONFLICT_RECONFIGURATION) {
                            scheduler.getMapping().getReconfBarrier().await();
                            scheduler.getMapping().getReconfBarrier().await();

                        }
                    } while (execQueue.goToNext());
                } catch (Exception ie) {
                    //ie.printStackTrace();
                    continue;
                }

            }

        }

    }
    
private class Receiver extends Thread{
    int rc_id;
    public Receiver(int id){
        this.rc_id=id;
    }
    public void run() {
        
            int cid; // ultimo cid de cada particao salvo nos metadados
            Socket[] connections = new Socket[2];
            System.out.println("started recovery of partition "+this.rc_id+ " at time = "+System.nanoTime());
            String filename = "map"+this.rc_id+".ser";
            String dataname = "map"+this.rc_id+"-metadata.txt"; 
            String[] ips = new String[2];
            ips[0] = "10.1.1.2";
            ips[1] = "10.1.1.3";          
            try{
                   // busca estado armazenado em disco
                   FileInputStream file = new FileInputStream(File.separator+"checkpoint"+paths[this.rc_id]+File.separator+"states"+File.separator+filename
                   ); 
                    ObjectInputStream in = new ObjectInputStream(file); 
                    Map<Integer,byte[]> m = new TreeMap<Integer,byte[]>();  
                    byte[] b = (byte[])in.readObject(); 
                    
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(bos);
                    oos.writeInt(BFTMapRequestType.RECOVERER);
                    oos.writeInt(this.rc_id);
                    oos.writeObject(m); 
                    oos.flush();
                    TOMMessage req = new TOMMessage(1, 1, 1, bos.toByteArray(), 1);
                    req.groupId= (Integer.toString(this.rc_id)+"#S").hashCode();
                    scheduler.schedule(new MessageContextPair(req, req.groupId, 0, null,null));          

                    //((SingleExecutable)executor).executeOrdered(bos.toByteArray(), null);
                    in.close(); 
                    file.close();
               }catch(Exception eeex){
                   //System.out.println("NO checkpoint for partition "+this.id);
               }
                
            //leitura de metadados e inicialização da lista de requisições
            Map<Integer,Integer> requests = new TreeMap<Integer,Integer>();
                try{
                    FileInputStream filet = new FileInputStream(File.separator+"checkpoint"+paths[this.rc_id]+File.separator+"metadata"+File.separator+dataname); 
                    BufferedReader br = new BufferedReader(new InputStreamReader(filet));
                    cid = Integer.parseInt(br.readLine());
                    requests.put(this.rc_id, 2);
                    //System.out.println("last cid for partition "+this.id+" = "+cid);
                    
                }catch(Exception eeerd){
                    //System.out.println("NO metadata for partition "+this.id);
                    cid=0;
                    requests.put(this.rc_id,2);
                }
               
        try {
            connections[0] = new Socket(ips[0],6666+this.rc_id);
            connections[1] = new Socket(ips[1],6666+this.rc_id);
            //requisitando metadados
            ObjectOutputStream os=null;
                
            for(int i =0;i<ips.length;i++){
                try {
                    os = new ObjectOutputStream(connections[i].getOutputStream());
                    os.writeObject(BFTMapRequestType.METADATA);
                    ObjectInputStream is = new ObjectInputStream(connections[i].getInputStream());
                    //System.out.println("REC");
                    Integer lastcid = (Integer) is.readObject();
                    //System.out.println("last cid for partition "+this.id+" of replica "+i+" is "+lastcid);
                    if(lastcid > cid){
                        //System.out.println("last chosen replica = "+i);
                        requests.put(this.rc_id, i);
                        cid=lastcid;
                    }
                } catch (IOException ex) {
                        Logger.getLogger(ParallelServiceReplica.class.getName()).log(Level.SEVERE, null, ex);
                } catch (ClassNotFoundException ex) {
                        Logger.getLogger(ParallelServiceReplica.class.getName()).log(Level.SEVERE, null, ex);
                }
                
            }
                    
            //requisitando estado
            connections[0] = new Socket(ips[0],6666+this.rc_id);
            connections[1] = new Socket(ips[1],6666+this.rc_id);
            
            if(requests.get(this.rc_id)!=id){
                ObjectOutputStream os2;
                try {
                    //System.out.println("thread "+this.rc_id+" asking partition "+this.rc_id+" from replica "+requests.get(this.rc_id));
                    os2 = new ObjectOutputStream(connections[requests.get(this.rc_id)].getOutputStream());
                    os2.writeObject(BFTMapRequestType.STATE);
                    ObjectInputStream is2 = new ObjectInputStream(connections[requests.get(this.rc_id)].getInputStream());
                    //System.out.println("REC");
                    byte[] state = (byte[]) is2.readObject();
                    //System.out.println("state size for partition "+this.rc_id+" of replica "+requests.get(this.rc_id)+" is "+state.length);
                    TOMMessage req = new TOMMessage(1, 1, 1, state, 1);
                    String gpid = Integer.toString(this.rc_id)+"#S";
                    req.groupId= gpid.hashCode();
                    System.out.println("gpid = "+gpid.hashCode());
                    scheduler.schedule(new MessageContextPair(req, req.groupId, 0, null,new MessageContext(cid, cid, TOMMessageType.REPLY, cid, scheduled, rc_id, rc_id, state, cid, cid, cid, rc_id, cid, cid, null, req, partition)));          
                    System.out.println("finished receivind checkpoint of partition "+this.rc_id+" at time " +System.nanoTime());
                    connections[0] = new Socket(ips[0],6666+this.rc_id);
                    connections[1] = new Socket(ips[1],6666+this.rc_id);
            
                    os2 = new ObjectOutputStream(connections[requests.get(this.rc_id)].getOutputStream());
                    os2.writeObject(666);
                    is2 = new ObjectInputStream(connections[requests.get(this.rc_id)].getInputStream());
                    //System.out.println("REC");
                    Queue<Operation> log = (Queue<Operation>) is2.readObject();
                    //System.out.println("state size for partition "+this.rc_id+" of replica "+requests.get(this.rc_id)+" is "+state.length);
                    //System.out.println("log size = "+log.size());
                    sleep(3000);
                    for(Operation o : log){
                        
                        byte[] b = ByteBuffer.allocate(1025).put(o.getContent(), 0, o.getContent().length).array();
                        
                        req = new TOMMessage(1, 1, 1, b, 1);
                        req.groupId= o.getClassId();
                        scheduler.schedule(new MessageContextPair(req, req.groupId, 0, null,new MessageContext(cid, cid, TOMMessageType.REPLY, cid, scheduled, rc_id, rc_id, state, cid, cid, cid, rc_id, cid, cid, null, req, partition)));          
                        statistics.computeStatistics(this.rc_id, 1);
                        tomLayer.setLastExec(o.getSequence());
                        
            
                    }
                    System.out.println("FINISHED processing log for partition "+this.rc_id+" at time"+System.nanoTime());
                } catch (Exception ex) {
                            System.out.println("XABU");
                            Logger.getLogger(ParallelServiceReplica.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            recovering=false;
        } catch (IOException ex) {
            Logger.getLogger(ParallelServiceReplica.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}

class RecoverThread extends Thread{
    public int rc_id;
    public byte[] state;
    public Integer metadata;
    public ServerSocket s;
           
        
    public RecoverThread(int id) throws IOException{
        this.rc_id = id;
        this.metadata=0;
        this.state=null;
        int port = this.rc_id+6666;
        this.s=  new ServerSocket(port);
        System.out.println("RC ID = "+this.rc_id);
        
    }
    public void run(){
        while(true){
            try {
                Socket recv = s.accept();
                //System.out.println("SOcket open");
                ObjectInputStream is = new ObjectInputStream(recv.getInputStream());
                ObjectOutputStream os = new ObjectOutputStream(recv.getOutputStream());
                //System.out.println(new String("//checkpoint"+paths[rc_id]+"//metadata//map"+this.rc_id+"-metadata.txt"));
                Integer op = (Integer) is.readObject();
                System.out.println("OP = "+op);
                if(op==BFTMapRequestType.METADATA){
                    String data = "";
                try{
                    data = new String(Files.readAllBytes(Paths.get("//checkpoint"+paths[rc_id]+"//metadata//map"+this.rc_id+"-metadata.txt"))); 
                    //System.out.println(data);
                  //  System.out.println("data = "+data);
                    data=data.replace("\n","").replace("\r", "");
                    data=data.replace("#", "");
                    int x = Integer.parseInt(data);
                    //System.out.println("X = "+x);
                    this.metadata=x;
                    System.out.println("sending metadata = "+this.metadata);
                    os.writeObject(this.metadata);
                    //recv.close();
                    //s.close();

                }catch(NoSuchFileException nofile){
                    this.metadata=0;
                    os.writeObject(this.metadata);
                    
                }   

                }else if(op==BFTMapRequestType.STATE){
                FileInputStream files;
                try {

                    String cpdir = File.separator+"checkpoint"+paths[rc_id]+File.separator+"states"+File.separator+"map"+this.rc_id+".ser";
                    InputStream in = this.getClass().getResourceAsStream(cpdir);
                    String dir = "/checkpoint"+paths[rc_id]+"/states/";
                    new File(dir).mkdirs();

                    files = new FileInputStream(cpdir);
                    ObjectInputStream ins = new ObjectInputStream(files); 
                    byte[] b = (byte[])ins.readObject();
                    ins.close();
                    InputStream inp = this.getClass().getResourceAsStream(File.separator+"checkpoint"+paths[rc_id]+File.separator+"metadata"+File.separator+"map"+this.rc_id+"-metadata.txt");
                    System.out.println("b size = "+b.length);
                    String dir2 = "/recovery/metadata/";
                    new File(dir2).mkdirs();
                    String filedir2 = dir+"map"+id+"-metadata.";
                    this.state = b;           
                    os.writeObject(b);
                    System.out.println("set?");
                    
                } catch (Exception ex) {
                    this.state=null;
                    
                }
                }else{
                    os.flush();
                    System.out.println("received log request");
                    Queue<Operation> log = new LinkedList<Operation>(workers[rc_id].log);
                    //MessageContextPair m = log.peek();
                    
                    
                    os.writeUnshared(log);
                }    
     
            } catch (IOException ex) {
                Logger.getLogger(RecoverThread.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(RecoverThread.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
}
}



