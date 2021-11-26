package parallelism;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.CyclicBarrier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.consensus.messages.MessageFactory;
import bftsmart.consensus.roles.Acceptor;
import bftsmart.consensus.roles.Proposer;
import bftsmart.reconfiguration.views.View;
import bftsmart.tom.MessageContext;
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
import bftsmart.tom.util.ShutdownHookThread;
import bftsmart.tom.util.TOMUtil;
import bftsmart.util.MultiOperationRequest;
import bftsmart.util.ThroughputStatistics;
import demo.bftmap.BFTMapRequestType;
import parallelism.scheduler.DefaultScheduler;
import parallelism.scheduler.ParallelScheduler;
import parallelism.scheduler.Scheduler;

public class ParallelServiceReplica extends ServiceReplica {

    private final Logger logger = LoggerFactory.getLogger(ParallelServiceReplica.class);
    private Receiver[] receivers;
    private int num_partition;
    private int error = 0;
    private boolean recovering = true;
    private boolean partition;
    protected Scheduler scheduler;
    private ThroughputStatistics statistics;
    private ServiceReplicaWorker[] workers;
    protected Map<String, MultiOperationCtx> ctxs = new Hashtable<>();
    private RecoverThread[] recoverers;
    private String[] paths;
    private int scheduled = 0;
    private int numDisks;

    public ParallelServiceReplica(int id, Executable executor, Recoverable recoverer, int initialWorkers, int period,
            boolean part, int numDisks) throws IOException, ClassNotFoundException {
        super(id, executor, null);

        this.numDisks = numDisks;
        this.partition = part;
        this.num_partition = initialWorkers;
        if (initialWorkers <= 0) {
            initialWorkers = 1;
        }

        if (partition) {
            logger.info("MULTI PARTITION WITH {} THREADS", initialWorkers);
            this.scheduler = new ParallelScheduler(this.id, initialWorkers, period);
        } else {
            logger.info("SINGLE PARTITION WITH {} THREADS", initialWorkers);
            this.scheduler = new DefaultScheduler(initialWorkers, period);
        }

        initWorkers(this.scheduler.getNumWorkers(), id);

        logger.info("Initializing recover threads");
        this.recoverers = new RecoverThread[initialWorkers];
        int count = 0;
        try {
            for (int i = 0; i < initialWorkers; i++) {
                recoverers[i] = new RecoverThread(this, i);
                recoverers[i].start();
                count++;
            }
            logger.info("Successfully initialized {} recover threads", count);
        } catch (IOException ex) {
            logger.error("Error trying to create recover threads", ex);
            throw new RuntimeException("Error trying to create recover threads", ex);
        }

        this.receivers = new Receiver[initialWorkers];
        for (int rec = 0; rec < initialWorkers; rec++) {
            this.receivers[rec] = new Receiver(this, rec, id, this.replicaCtx);
            this.receivers[rec].start();
        }
        for (int rec = 0; rec < initialWorkers; rec++) {
            try {
                this.receivers[rec].join();
            } catch (InterruptedException e) {
                throw new RuntimeException("Receiver interrupted...");
            }
        }
        logger.info("Receiver threads initialized!");
    }

    public void setRecovering(boolean recovering) {
        this.recovering = recovering;
    }

    private String getPartitionPath(final int threadIndex) {
        StringBuilder sb = new StringBuilder();
        sb.append(File.separatorChar).append("disk").append(threadIndex % this.numDisks).append(File.separatorChar)
                .append("checkpoint").append(threadIndex % this.numDisks);
        return sb.toString();
    }

    protected void initWorkers(int n, int id) {
        this.workers = new ServiceReplicaWorker[n];
        statistics = new ThroughputStatistics(n, id);
        statistics.start();
        int tid = 0;
        for (int i = 0; i < n; i++) {
            logger.info("Initializing Servica Replica Worker({})", i);
            workers[i] = new ServiceReplicaWorker(this, (FIFOQueue) this.scheduler.getMapping().getAllQueues()[i], tid);
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

    public void receiveMessages(int consId[], int regencies[], int leaders[], CertifiedDecision[] cDecs,
            TOMMessage[][] requests) {

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
        // logger.info("Received message");
        for (TOMMessage[] requestsFromConsensus : requests) {
            TOMMessage firstRequest = requestsFromConsensus[0];
            int requestCount = 0;
            noop = true;
            for (TOMMessage request : requestsFromConsensus) {
                logger.debug(
                        "(ServiceReplica.receiveMessages) Processing TOMMessage from "
                                + "client {} with sequence number {} for session {} decided in consensus {}",
                        request.getSender(), request.getSequence(), request.getSession(), consId[consensusCount]);

                if (request.getViewID() == SVController.getCurrentViewId()) {
                    if (request.getReqType() == TOMMessageType.ORDERED_REQUEST) {
                        noop = false;
                        // numRequests++;

                        request.deliveryTime = System.nanoTime();
                        MessageContextPair msg = null;
                        if (recovering) {
                            MultiOperationRequest reqs = new MultiOperationRequest(request.getContent());
                            msg = new MessageContextPair(request, request.groupId, 0, reqs.operations[0].data, null);

                        } else {
                            MultiOperationRequest reqs = new MultiOperationRequest(request.getContent());
                            MultiOperationCtx ctx = new MultiOperationCtx(reqs.operations.length, request);
                            this.ctxs.put(request.toString(), ctx);
                            MessageContext m = new MessageContext(request.getSender(), request.getViewID(),
                                    request.getReqType(), request.getSession(), request.getSequence(),
                                    request.getOperationId(), request.getReplyServer(),
                                    request.serializedMessageSignature, firstRequest.timestamp, request.numOfNonces,
                                    request.seed, regencies[consensusCount], leaders[consensusCount],
                                    consId[consensusCount], cDecs[consensusCount].getConsMessages(), firstRequest,
                                    true);
                            m.setLastInBatch();
                            msg = new MessageContextPair(request, request.groupId, 0, reqs.operations[0].data, m);
                        }

                        // if(msg.request.getSequence()>this.tomLayer.getLastExec() &&
                        // this.tomLayer.getLastExec()!=0)
                        this.scheduler.schedule(msg);
                        // logger.info("scheduled");
                        // this.scheduled++;
                        // if(scheduled>=200000 && this.id==0){
                        // logger.info("RESTARTING REPLICA");
                        // this.restart();
                        //
                        // }
                        //
                        // implementar log de requests

                    } else if (request.getReqType() == TOMMessageType.RECONFIG) {

                        SVController.enqueueUpdate(request);
                    } else {
                        logger.error("Should never reach here!");
                        throw new RuntimeException("Should never reach here! ");
                    }
                } else if (request.getViewID() < SVController.getCurrentViewId()) {
                    // message sender had an old view, resend the message to
                    // him (but only if it came from consensus an not state transfer)
                    tomLayer.getCommunication().send(new int[] { request.getSender() },
                            new TOMMessage(SVController.getStaticConf().getProcessId(), request.getSession(),
                                    request.getSequence(), TOMUtil.getBytes(SVController.getCurrentView()),
                                    SVController.getCurrentViewId()));

                }

                requestCount++;
            }
            consensusCount++;
            // logger.info("globalC = "+globalC);
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

    public void setLastExec(int last) {
        this.tomLayer.setLastExec(last);
    }

    class ServiceReplicaWorker extends Thread {

        /**
         *
         */
        private final ParallelServiceReplica parallelServiceReplica;
        private Logger logger = LoggerFactory.getLogger(ServiceReplicaWorker.class);
        private FIFOQueue<MessageContextPair> requests;
        private int thread_id;
        private Checkpointer checkpointer;
        private FIFOQueue<Integer> interacoes = new FIFOQueue<>();
        public Queue<Operation> log;

        public ServiceReplicaWorker(ParallelServiceReplica parallelServiceReplica,
                FIFOQueue<MessageContextPair> requests, int id) {
            this.parallelServiceReplica = parallelServiceReplica;
            this.log = new LinkedList<Operation>();
            this.thread_id = id;
            this.requests = requests;
            this.checkpointer = new Checkpointer(this.parallelServiceReplica, this.thread_id);
            this.checkpointer.start();

            logger.info("Service replica worker initialized, queue {}", requests.hashCode());
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
                        HibridClassToThreads ct = this.parallelServiceReplica.scheduler.getClasses().get(msg.classId);
                        ByteArrayInputStream in = new ByteArrayInputStream(msg.request.getContent());
                        DataInputStream dis = new DataInputStream(in);
                        int cmd = dis.readInt();

                        logger.debug("Processing message with ct.type {} and cmd {}", ct.type, cmd);

                        if (cmd == BFTMapRequestType.RECOVERER) {
                            msg.resp = ((SingleExecutable) this.parallelServiceReplica.executor)
                                    .executeOrdered(msg.request.getContent(), msg.m);

                        } else if (cmd == BFTMapRequestType.RECOVERY_FINISHED) {
                            logger.info("Recovery process finished! Log has {} operations already stored.", this.log.size());
                            this.parallelServiceReplica.setRecovering(false);

                        } else if (ct.type == ClassToThreads.CONC) {

                            msg.resp = ((SingleExecutable) this.parallelServiceReplica.executor)
                                    .executeOrdered(msg.request.getContent(), msg.m);
                            if (!this.parallelServiceReplica.recovering) {
                                MultiOperationCtx ctx = this.parallelServiceReplica.ctxs.get(msg.request.toString());

                                this.log.add(new Operation(cmd, msg.classId, msg.request.getContent(),
                                        msg.request.getSequence()));
                                // logger.info("log peek = "+log.peek().classID);
                                if (ctx != null) {
                                    ctx.add(msg.index, msg.resp);
                                    if (ctx.response.isComplete() && !ctx.finished
                                            && (ctx.interger.getAndIncrement() == 0)) {
                                        ctx.finished = true;
                                        ctx.request.reply = new TOMMessage(this.parallelServiceReplica.id,
                                                ctx.request.getSession(), ctx.request.getSequence(), msg.resp,
                                                this.parallelServiceReplica.SVController.getCurrentViewId());
                                        this.parallelServiceReplica.replier.manageReply(ctx.request, msg.m);

                                    }
                                    this.parallelServiceReplica.statistics.computeStatistics(thread_id, 1);
                                }
                            }

                        } else if ((ct.type == ClassToThreads.SYNC && ct.tIds.length == 1)) {// SYNC mas só com 1
                                                                                             // thread, não precisa usar
                                                                                             // barreira

                            if (cmd == BFTMapRequestType.CKP) {
                                synchronized (this.checkpointer) {
                                    logger.info(
                                            "Got a checkpoint command in ClassToThreads.SYNC and threadIds.lenght = 1");
                                    checkpointer.addRequest(msg);
                                    this.checkpointer.notify();
                                    this.checkpointer.wait();
                                    this.log.clear();
                                }
                            } else {
                                msg.resp = ((SingleExecutable) this.parallelServiceReplica.executor)
                                        .executeOrdered(msg.request.getContent(), msg.m);
                                if (!this.parallelServiceReplica.recovering) {
                                    this.log.add(new Operation(cmd, msg.classId, msg.request.getContent(),
                                            msg.request.getSequence()));
                                    //logger.info("log peek = "+log.peek());
                                    MultiOperationCtx ctx = this.parallelServiceReplica.ctxs
                                            .get(msg.request.toString());
                                    //logger.info("CMD = "+cmd);
                                    if (ctx != null) {
                                        ctx.add(msg.index, msg.resp);
                                        if (ctx.response.isComplete() && !ctx.finished
                                                && (ctx.interger.getAndIncrement() == 0)) {
                                            ctx.finished = true;
                                            ctx.request.reply = new TOMMessage(this.parallelServiceReplica.id,
                                                    ctx.request.getSession(), ctx.request.getSequence(), msg.resp,
                                                    this.parallelServiceReplica.SVController.getCurrentViewId());
                                            this.parallelServiceReplica.replier.manageReply(ctx.request, msg.m);
                                        }
                                        this.parallelServiceReplica.statistics.computeStatistics(thread_id, 1);
                                    }
                                }
                            }

                        } else if (ct.type == ClassToThreads.SYNC) {
                            if (thread_id == this.parallelServiceReplica.scheduler.getMapping()
                                    .getExecutorThread(msg.classId)) {

                                this.parallelServiceReplica.scheduler.getMapping().getBarrier(msg.classId).await();
                                if (cmd == BFTMapRequestType.CKP) {

                                    synchronized (this.checkpointer) {
                                        checkpointer.addRequest(msg);
                                        this.checkpointer.notify();
                                        this.checkpointer.wait();
                                        this.log.clear();
                                    }

                                } else {
                                    msg.resp = ((SingleExecutable) this.parallelServiceReplica.executor)
                                            .executeOrdered(msg.request.getContent(), msg.m);
                                    this.log.add(new Operation(cmd, msg.classId, msg.request.getContent(),
                                            msg.request.getSequence()));
                                    MultiOperationCtx ctx = this.parallelServiceReplica.ctxs
                                            .get(msg.request.toString());
                                    if (ctx != null) {
                                        ctx.add(msg.index, msg.resp);
                                        if (ctx.response.isComplete() && !ctx.finished
                                                && (ctx.interger.getAndIncrement() == 0)) {
                                            ctx.finished = true;
                                            ctx.request.reply = new TOMMessage(this.parallelServiceReplica.id,
                                                    ctx.request.getSession(), ctx.request.getSequence(), msg.resp,
                                                    this.parallelServiceReplica.SVController.getCurrentViewId());
                                            this.parallelServiceReplica.replier.manageReply(ctx.request, msg.m);
                                        }
                                    }
                                }
                                this.parallelServiceReplica.statistics.computeStatistics(thread_id, 1);
                                this.parallelServiceReplica.scheduler.getMapping().getBarrier(msg.classId).await();
                            } else {
                                this.parallelServiceReplica.scheduler.getMapping().getBarrier(msg.classId).await();
                                if (cmd == BFTMapRequestType.CKP && this.parallelServiceReplica.partition) {
                                    synchronized (this.checkpointer) {
                                        logger.info(
                                                "Got a checkpoint command in ClassToThreads.SYNC and it's not the class executor");
                                        checkpointer.addRequest(msg);
                                        this.checkpointer.notify();
                                        this.checkpointer.wait();
                                        this.log.clear();
                                    }
                                }
                                this.parallelServiceReplica.scheduler.getMapping().getBarrier(msg.classId).await();
                            }

                        } else if (msg.classId == ParallelMapping.CONFLICT_RECONFIGURATION) {
                            this.parallelServiceReplica.scheduler.getMapping().getReconfBarrier().await();
                            this.parallelServiceReplica.scheduler.getMapping().getReconfBarrier().await();

                        }
                        

                    } while (execQueue.goToNext());
                    logger.debug("No more queued data");
                } catch (Exception ex) {
                    logger.error("Error running thread", ex);
                }
            }

        }

    }

    class RecoverHandlerThread extends Thread {

        private final Logger logger = LoggerFactory.getLogger(RecoverHandlerThread.class);
        private final int rc_id;
        private byte[] state;
        private Integer metadata;
        private final Socket client;
        private final ParallelServiceReplica parallelServiceReplica;

        public RecoverHandlerThread(ParallelServiceReplica parallelServiceReplica, int rc_id, byte[] state,
                int metadata, Socket client) {
            this.parallelServiceReplica = parallelServiceReplica;
            this.rc_id = rc_id;
            this.state = state;
            this.metadata = metadata;
            this.client = client;
            logger.info("New connection from {}", client.getInetAddress());
        }

        @Override
        public void run() {
            try {
                ObjectInputStream is = new ObjectInputStream(client.getInputStream());
                ObjectOutputStream os = new ObjectOutputStream(client.getOutputStream());
                int op;
                while (true) {
                    try {
                        op = is.readInt();
                    } catch (EOFException e) {
                        logger.info("Nothing to read from {}, closing socket", this.client.getInetAddress());
                        client.close();
                        return;
                    }
                    if (op == BFTMapRequestType.METADATA) {
                        logger.info("Recover received a request recover operation of type: METADATA");
                        String data = "";
                        try {
                            String metadataFile;
                            if (this.parallelServiceReplica.partition) {
                                metadataFile = getPartitionPath(this.rc_id) + File.separator + "metadata"
                                        + File.separator + "map" + this.rc_id + "-metadata.txt";
                            } else {
                                metadataFile = getPartitionPath(this.rc_id) + File.separator + "metadata"
                                        + File.separator + "ALLmap-metadata.txt";
                            }
                            data = new String(Files.readAllBytes(Paths.get(metadataFile)));
                            data = data.replace("\n", "").replace("\r", "");
                            data = data.replace("#", "");
                            int x = Integer.parseInt(data);
                            this.metadata = x;
                            logger.info("Sending metadata({}) of partition {}", this.metadata, this.rc_id);
                            os.writeInt(this.metadata);
                        } catch (NoSuchFileException nofile) {
                            logger.warn("No metadata file to send");
                            this.metadata = 0;
                            os.writeInt(this.metadata);
                        }

                    } else if (op == BFTMapRequestType.STATE) {
                        logger.info("Recover received a request recover operation of type: STATE");
                        String cpdir, dir;
                        if (this.parallelServiceReplica.partition) {
                            cpdir = getPartitionPath(this.rc_id) + File.separator + "states" + File.separator + "map"
                                    + this.rc_id + ".ser";
                            dir = getPartitionPath(this.rc_id) + File.separator + "states" + File.separator;
                        } else {
                            cpdir = getPartitionPath(this.rc_id) + File.separator + "states" + File.separator + "ALLmap"
                                    + this.rc_id + ".ser";
                            dir = getPartitionPath(this.rc_id) + File.separator + "states" + File.separator;
                        }
                        new File(dir).mkdirs();

                        Object checkpointState = null;
                        try (ObjectInputStream ins = new ObjectInputStream(new FileInputStream(cpdir))) {
                            checkpointState = ins.readObject();
                            this.state = (byte[]) checkpointState;
                            os.writeObject(this.state);
                            logger.info("Sending checkpoint of partition {} with {} bytes", this.rc_id,
                                    this.state.length);
                        } catch (FileNotFoundException ex) {
                            logger.warn("There is no checkpoint file to send, sending empty object");
                            checkpointState = new byte[0];
                            os.writeObject(checkpointState);
                        } catch (IOException ex) {
                            logger.error("Failed reading local states files", ex);
                        }
                    } else if (op == BFTMapRequestType.LOG) {
                        Queue<Operation> log = new LinkedList<Operation>(
                                this.parallelServiceReplica.workers[rc_id].log);
                        logger.info("Sending log with {} operations", log.size());
                        os.writeObject(log);
                    }
                    os.flush();
                }
            } catch (IOException | ClassNotFoundException ex) {
                logger.error("Failure handling recover operation", ex);
            }
        }
    }

    class RecoverThread extends Thread {

        private final Logger logger = LoggerFactory.getLogger(RecoverThread.class);
        private final ParallelServiceReplica parallelServiceReplica;
        private int rc_id;
        private byte[] state;
        private Integer metadata;
        private ServerSocket s;

        public RecoverThread(ParallelServiceReplica parallelServiceReplica, int id) throws IOException {
            this.parallelServiceReplica = parallelServiceReplica;
            this.rc_id = id;
            this.metadata = 0;
            this.state = null;
            this.s = new ServerSocket();
            int port = this.rc_id + 6666;
            this.s.setReuseAddress(true);
            this.s.bind(new InetSocketAddress(port));
            logger.info("Recover thread ID {} at port {}", this.rc_id, port);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    new RecoverHandlerThread(parallelServiceReplica, rc_id, state, metadata, this.s.accept()).start();
                }
            } catch (IOException ex) {
                logger.error("Failed to create server recover thread socket", ex);
            }

        }

    }

    class Receiver extends Thread {
        /**
         *
         */
        private final Logger logger = LoggerFactory.getLogger(Receiver.class);
        private final ParallelServiceReplica parallelServiceReplica;
        private int rc_id;
        private int processId;
        private ReplicaContext replicaContext;

        public Receiver(ParallelServiceReplica parallelServiceReplica, int id, int processId,
                ReplicaContext replicaContext) {
            this.parallelServiceReplica = parallelServiceReplica;
            this.rc_id = id;
            this.processId = processId;
            this.replicaContext = replicaContext;
        }

        @Override
        public void run() {

            int cid; // ultimo cid de cada particao salvo nos metadados
            logger.info("Started recovery of partition {}", this.rc_id);
            String filename = "map" + this.rc_id + ".ser";
            String dataname = "map" + this.rc_id + "-metadata.txt";

            boolean stateFound = false;

            try (FileInputStream file = new FileInputStream(
                    getPartitionPath(this.rc_id) + File.separator + "states" + File.separator + filename);
                    ObjectInputStream in = new ObjectInputStream(file)) {
                logger.info("Restoring state from local storage");
                Map<Integer, byte[]> m = new TreeMap<Integer, byte[]>();
                byte[] b = (byte[]) in.readObject();

                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeInt(BFTMapRequestType.RECOVERER);
                oos.writeInt(this.rc_id);
                oos.writeObject(m);
                oos.flush();
                TOMMessage req = new TOMMessage(1, 1, 1, bos.toByteArray(), 1);
                req.groupId = (Integer.toString(this.rc_id) + "#S").hashCode();
                this.parallelServiceReplica.scheduler.schedule(new MessageContextPair(req, req.groupId, 0, null, null));

                // ((SingleExecutable)executor).executeOrdered(bos.toByteArray(), null);
                logger.info("State restored from local storage");
                stateFound = true;
            } catch (IOException | ClassNotFoundException ex) {
                logger.warn("NO local checkpoint for partition {} found", this.rc_id);
            }

            // leitura de metadados e inicialização da lista de requisições
            Map<Integer, Integer> requests = new TreeMap<Integer, Integer>();
            try (FileInputStream filet = new FileInputStream(
                    getPartitionPath(this.rc_id) + File.separator + "metadata" + File.separator + dataname);
                    BufferedReader br = new BufferedReader(new InputStreamReader(filet))) {
                logger.info("Restoring metadata from local storage");
                cid = Integer.parseInt(br.readLine());
                requests.put(this.rc_id, 2);
                logger.info("Sucessfully restored metadata from local storage");
                // @author: henriquedsg
                if (stateFound) {// metadata and state found locally
                    this.parallelServiceReplica.recovering = false;
                    return;
                }
            } catch (Exception eeerd) {
                logger.warn("NO local metadata for partition {} ", this.rc_id);
                cid = 0;
                requests.put(this.rc_id, this.parallelServiceReplica.id);
            }

            View currentView = this.replicaContext.getCurrentView();
            int numOfProcesses = currentView.getProcesses().length;
            Socket[] sockets = new Socket[numOfProcesses];
            ObjectOutputStream[] oss = new ObjectOutputStream[numOfProcesses];
            ObjectInputStream[] iss = new ObjectInputStream[numOfProcesses];
            int port;
            InetAddress add;
            for (int i = 0; i < numOfProcesses; i++) {
                if (processId != currentView.getProcesses()[i]) {
                    add = currentView.getAddress(currentView.getProcesses()[i]).getAddress();
                    port = 6666 + this.rc_id;
                    int tries = 1;
                    while (tries <= 3) {
                        try {
                            logger.info("Connecting to {}", add);
                            sockets[i] = new Socket();
                            sockets[i].connect(new InetSocketAddress(add, port));
                            tries = 999;
                        } catch (Exception ie) {
                            if (tries > 3) {
                                logger.error("Error connecting to {}:{}", add, port, ie);
                                return;
                            } else {
                                try {
                                    logger.warn("Retrying to connect to {}:{}...", add, port);
                                    Thread.sleep(4000 * (2 ^ tries));
                                } catch (InterruptedException iee) {}
                            }

                        }
                        tries++;
                    }

                    logger.info("Socket connected to {}", sockets[i].getInetAddress());
                    // requisitando metadados
                    try {
                        oss[i] = new ObjectOutputStream(sockets[i].getOutputStream());
                        iss[i] = new ObjectInputStream(sockets[i].getInputStream());

                        logger.info("Requesting metada to {}", sockets[i].getRemoteSocketAddress());
                        oss[i].writeInt(BFTMapRequestType.METADATA);
                        oss[i].flush();

                        logger.info("Reading metadata's request reply from {}", sockets[i].getRemoteSocketAddress());
                        int lastcid = iss[i].readInt();
                        if (lastcid > cid) {
                            requests.put(this.rc_id, i);
                            cid = lastcid;
                        }
                    } catch (IOException ex) {
                        logger.error("Receiver thread failed while requesting metadata to {}",
                                sockets[i].getRemoteSocketAddress(), ex);
                        return;
                    }
                }
            }

            // requisitando estado
            if (requests.get(this.rc_id) != this.parallelServiceReplica.id) {
                ObjectOutputStream os2 = oss[requests.get(this.rc_id)];
                ObjectInputStream is2 = iss[requests.get(this.rc_id)];

                try {
                    logger.info("Requesting checkpoint state to {} of partition {}",
                            sockets[requests.get(this.rc_id)].getRemoteSocketAddress(), this.rc_id);
                    os2.writeInt(BFTMapRequestType.STATE);
                    os2.flush();

                    ByteArrayOutputStream bos;
                    DataOutputStream dos;

                    byte[] state = (byte[]) is2.readObject();
                    if (state.length == 0) {
                        logger.info("Received an empty checkpoint for partition {}, ignoring recovery process",
                                this.rc_id);
                    } else {
                        logger.info("Received the checkpoint of partition {} with size {}", this.rc_id, state.length);

                        logger.info("Scheduling checkpoint installation with rc_id {}", this.rc_id);

                        bos = new ByteArrayOutputStream();
                        dos = new DataOutputStream(bos);
                        dos.writeInt(BFTMapRequestType.RECOVERER);
                        dos.flush();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeInt(this.rc_id);
                        oos.writeObject(state);
                        oos.flush();
                        bos.flush();
                        
                        TOMMessage req = new TOMMessage(this.parallelServiceReplica.id, 1, 1, bos.toByteArray(), 1);
                        req.groupId = (Integer.toString(this.rc_id) + "#S").hashCode();
                        this.parallelServiceReplica.scheduler
                                .schedule(new MessageContextPair(req, req.groupId, 0, null, new MessageContext(cid, cid, TOMMessageType.REPLY, cid,
                                this.parallelServiceReplica.scheduled, rc_id, rc_id, state, cid, cid, cid,
                                rc_id, cid, cid, null, req, this.parallelServiceReplica.partition)));
                        
                        logger.info("Requesting log of partition {}", this.rc_id);
                        os2.writeInt(BFTMapRequestType.LOG);
                        os2.flush();

                        Queue<Operation> log = (Queue<Operation>) is2.readObject();
                        logger.info("Received log of partition {} with {} operations", this.rc_id, log.size());

                        logger.info("Starting to process log of partition {}", this.rc_id);
                        for (Operation o : log) {
                            byte[] b = ByteBuffer.allocate(o.getContent().length)
                                    .put(o.getContent(), 0, o.getContent().length).array();

                            
                            req = new TOMMessage(this.parallelServiceReplica.id, 1, 1, b, 1);
                            req.groupId = o.getClassId();
                            this.parallelServiceReplica.scheduler.schedule(new MessageContextPair(req, o.getClassId(),
                                    0, null,
                                    new MessageContext(cid, cid, TOMMessageType.REPLY, cid,
                                            this.parallelServiceReplica.scheduled, rc_id, rc_id, state, cid, cid, cid,
                                            rc_id, cid, cid, null, req, this.parallelServiceReplica.partition)));
                            this.parallelServiceReplica.statistics.computeStatistics(this.rc_id, 1);
                            this.parallelServiceReplica.setLastExec(o.getSequence());
                            
                        }

                    }

                    bos = new ByteArrayOutputStream();
                    dos = new DataOutputStream(bos);
                    dos.writeInt(BFTMapRequestType.RECOVERY_FINISHED);
                    dos.flush();
                    bos.flush();
                    
                    TOMMessage req = new TOMMessage(this.parallelServiceReplica.id, 1, 1, bos.toByteArray(), 1);
                    req.groupId = (Integer.toString(this.rc_id) + "#S").hashCode();
                    this.parallelServiceReplica.scheduler.schedule(new MessageContextPair(req, req.groupId, 0, null,
                            null));

                } catch (Exception ex) {
                    logger.warn("Failed requesting state to another replica.", ex);
                    this.parallelServiceReplica.recovering = false;
                } finally {
                    try {
                        os2.close();
                        is2.close();
                    } catch (Exception ex) {
                    }
                }
            }

            for (Socket socket : sockets) {
                try {
                    if (null != socket)
                        socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class Checkpointer extends Thread {
        /**
         *
         */
        private final Logger logger = LoggerFactory.getLogger(Checkpointer.class);
        private final ParallelServiceReplica parallelServiceReplica;
        private int cp_id;
        private boolean doCP;
        // public byte[] req = null;
        private MessageContextPair req = null;

        // public Queue<byte[]> requests;
        private Queue<MessageContextPair> requests;

        public Checkpointer(ParallelServiceReplica parallelServiceReplica, int id) {
            this.parallelServiceReplica = parallelServiceReplica;
            this.cp_id = id;
            this.requests = new LinkedList<MessageContextPair>();
            this.doCP = false;
        }

        @Override
        public void run() {
            logger.info("Checkpoint thread {} initialized, using partitions = {}", this.cp_id,
                    this.parallelServiceReplica.partition);
            while (true) {
                synchronized (this) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    logger.info("Initializing checkpointing procedure");
                    while (this.requests.size() > 0) {
                        req = this.requests.poll();
                        byte[] b;
                        int cid = 0;
                        String filename;
                        String dataname;
                        if (this.parallelServiceReplica.partition) {
                            ByteArrayInputStream in = new ByteArrayInputStream(req.request.getContent());

                            try {
                                int n = new DataInputStream(in).readInt();
                                String s = new DataInputStream(in).readUTF();
                                cid = new DataInputStream(in).readInt();

                            } catch (IOException ex) {
                                logger.error("Checkpointer failed while reading input streams", ex);
                                System.exit(-1);
                            }
                            ByteArrayOutputStream out = new ByteArrayOutputStream();
                            DataOutputStream dos = new DataOutputStream(out);
                            String s = Integer.toString(this.cp_id);
                            try {
                                dos.writeInt(BFTMapRequestType.CKP);
                                dos.writeUTF(s);
                            } catch (IOException ex) {
                                logger.error("Checkpointer failed while writing to output streams", ex);
                            }

                            b = ((SingleExecutable) this.parallelServiceReplica.executor)
                                    .executeOrdered(out.toByteArray(), req.m);
                            filename = "map" + this.cp_id + ".ser";
                            dataname = "map" + this.cp_id + "-metadata.txt";
                            // logger.info("array size = "+req.length);

                            try {
                                FileOutputStream fileOut =
                                        // new
                                        // FileOutputStream(File.separator+"recovery"+File.separator+"states"+File.separator+filename);
                                        new FileOutputStream(getPartitionPath(this.cp_id) + File.separator + "states"
                                                + File.separator + filename);

                                ObjectOutputStream out1 = new ObjectOutputStream(fileOut);
                                out1.writeObject(b);
                                out1.close();
                                fileOut.close();
                                BufferedWriter writer =
                                        // new BufferedWriter(new
                                        // FileWriter(File.separator+"recovery"+File.separator+"metadata"+File.separator+dataname));
                                        new BufferedWriter(new FileWriter(getPartitionPath(this.cp_id) + File.separator
                                                + "metadata" + File.separator + dataname));

                                writer.write(Integer.toString(cid));
                                writer.write("\n#");
                                writer.close();
                                this.notify();
                            } catch (Exception ex) {
                                logger.error("Error trying to write checkpoint", ex);
                                System.exit(-1);
                            }

                        } else {
                            b = ((SingleExecutable) this.parallelServiceReplica.executor)
                                    .executeOrdered(req.request.getContent(), req.m);
                            filename = "ALLmap" + this.cp_id + ".ser";
                            dataname = "ALLmap-metadata.txt";

                            try (FileOutputStream fileOut =
                                    // new
                                    // FileOutputStream(File.separator+"recovery"+File.separator+"states"+File.separator+filename);
                                    new FileOutputStream(getPartitionPath(this.cp_id) + File.separator + "states"
                                            + File.separator + filename);
                                    ObjectOutputStream out1 = new ObjectOutputStream(fileOut);
                                    BufferedWriter writer =
                                            // new BufferedWriter(new
                                            // FileWriter(File.separator+"recovery"+File.separator+"metadata"+File.separator+dataname));
                                            new BufferedWriter(new FileWriter(getPartitionPath(this.cp_id)
                                                    + File.separator + "metadata" + File.separator + dataname))) {

                                out1.writeObject(b);
                                out1.flush();

                                writer.write(Integer.toString(cid));
                                writer.write("\n#");
                                this.notify();
                            } catch (Exception ex) {
                                logger.error("Error trying to write checkpoint", ex);
                                System.exit(-1);
                            }
                        }

                    }
                    logger.info("Checkpointing has finished");
                }
            }
        }

        public void addRequest(MessageContextPair m) {
            // logger.info(this.id+" added to queue");
            this.requests.add(m);

        }
    }
}
