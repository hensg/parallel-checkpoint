package parallelism;

import bftsmart.reconfiguration.views.View;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.server.Executable;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.tom.util.TOMUtil;
import bftsmart.util.MultiOperationRequest;
import bftsmart.util.ThroughputStatistics;
import bftsmart.util.ThroughputStatistics2;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.InputChunked;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.OutputChunked;
import demo.bftmap.BFTMapRequestType;
import demo.bftmap.BFTMapServerMP;

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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parallelism.scheduler.DefaultScheduler;
import parallelism.scheduler.ParallelScheduler;
import parallelism.scheduler.Scheduler;

public class ParallelServiceReplica extends ServiceReplica {

    private final Logger logger = LoggerFactory.getLogger(ParallelServiceReplica.class);
    private Receiver[] receivers;
    private int num_partition;
    private int error = 0;
    private boolean partition;
    private boolean recovering = true;
    private List<MessageContextPair> msgBuffer = new ArrayList<>();
    public Scheduler scheduler;
    private ThroughputStatistics2 statistics;
    private ServiceReplicaWorker[] workers;
    protected Map<String, MultiOperationCtx> ctxs = new Hashtable<>();
    private RecoverThread[] recoverers;
    private String[] paths;
    private int scheduled = 0;
    private int numDisks;
    protected AtomicInteger numCheckpointsExecuted = new AtomicInteger();
    ScheduledExecutorService statisticsThreadExecutor = Executors.newSingleThreadScheduledExecutor();

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

        statistics = new ThroughputStatistics2(this.scheduler.getNumWorkers(), id);
        statisticsThreadExecutor.scheduleAtFixedRate(statistics, 0, 1, TimeUnit.SECONDS);

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
        try {
            for (int rec = 0; rec < initialWorkers; rec++) {
                this.receivers[rec].join();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Receiver interrupted...");
        }
        logger.info("Receiver threads initialized!");
        recovering = false;
        initWorkers(this.scheduler.getNumWorkers(), id);
    }

    private String getPartitionPath(final int threadIndex) {
        StringBuilder sb = new StringBuilder();
        sb.append(File.separatorChar)
                .append("disk")
                .append(threadIndex % this.numDisks)
                .append(File.separatorChar)
                .append("checkpoint")
                .append(threadIndex % this.numDisks);
        return sb.toString();
    }

    protected void initWorkers(int n, int id) {
        this.workers = new ServiceReplicaWorker[n];
        int tid = 0;
        for (int i = 0; i < n; i++) {
            logger.info("Initializing Service Replica Worker({})", i);
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
                logger.debug("(ServiceReplica.receiveMessages) Processing TOMMessage from "
                        + "client {} with sequence number {} for session {} decided in consensus {}",
                        request.getSender(), request.getSequence(), request.getSession(), consId[consensusCount]);

                if (request.getViewID() == SVController.getCurrentViewId()) {
                    if (request.getReqType() == TOMMessageType.ORDERED_REQUEST) {
                        noop = false;
                        // numRequests++;

                        request.deliveryTime = System.nanoTime();
                        MessageContextPair msg = null;
                        MultiOperationRequest reqs = new MultiOperationRequest(request.getContent());
                        MultiOperationCtx ctx = new MultiOperationCtx(reqs.operations.length, request);
                        this.ctxs.put(request.toString(), ctx);
                        MessageContext m = new MessageContext(
                                request.getSender(), request.getViewID(), request.getReqType(), request.getSession(),
                                request.getSequence(), request.getOperationId(), request.getReplyServer(),
                                request.serializedMessageSignature, firstRequest.timestamp, request.numOfNonces,
                                request.seed, regencies[consensusCount], leaders[consensusCount],
                                consId[consensusCount],
                                cDecs[consensusCount].getConsMessages(), firstRequest, true);
                        m.setLastInBatch();
                        msg = new MessageContextPair(request, request.groupId, 0, reqs.operations[0].data, m);
                        this.scheduler.schedule(msg);

                    } else if (request.getReqType() == TOMMessageType.RECONFIG) {

                        SVController.enqueueUpdate(request);
                    } else {
                        logger.error("Should never reach here!");
                        throw new RuntimeException("Should never reach here! ");
                    }
                } else if (request.getViewID() < SVController.getCurrentViewId()) {
                    // message sender had an old view, resend the message to
                    // him (but only if it came from consensus an not state
                    // transfer)
                    tomLayer.getCommunication().send(new int[] { request.getSender() },
                            new TOMMessage(SVController.getStaticConf().getProcessId(),
                                    request.getSession(), request.getSequence(),
                                    TOMUtil.getBytes(SVController.getCurrentView()),
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

    public void setLastExec(int last) {
        this.tomLayer.setLastExec(last);
    }

    public int getLastExec() {
        return this.tomLayer.getLastExec();
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

            logger.info("Service replica worker initialized, queue {}", requests.hashCode());
        }

        int localC = 0;
        int localTotal = 0;

        private void executeConcurrentOperation(int cmd, MessageContextPair msg) throws Exception {
            msg.resp = ((SingleExecutable) this.parallelServiceReplica.executor)
                    .executeOrdered(msg.request.getContent(), msg.m);

            MultiOperationCtx ctx = this.parallelServiceReplica.ctxs.get(msg.request.toString());
            Operation op = new Operation(cmd, msg.classId, msg.request.getContent(), msg.request.getSequence());
            this.log.add(op);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(op);
            oos.flush();
            if (ctx != null) {
                ctx.add(msg.index, msg.resp);
                if (ctx.response.isComplete() && !ctx.finished && (ctx.interger.getAndIncrement() == 0)) {
                    ctx.finished = true;
                    ctx.request.reply = new TOMMessage(this.parallelServiceReplica.id, ctx.request.getSession(),
                            ctx.request.getSequence(), msg.resp,
                            this.parallelServiceReplica.SVController.getCurrentViewId());
                    this.parallelServiceReplica.replier.manageReply(ctx.request, msg.m);
                }
            }
        }

        private void executeSyncOperation(int cmd, MessageContextPair msg) throws Exception {
            msg.resp = ((SingleExecutable) this.parallelServiceReplica.executor)
                    .executeOrdered(msg.request.getContent(), msg.m);
            Operation op = new Operation(cmd, msg.classId, msg.request.getContent(), msg.request.getSequence());
            this.log.add(op);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(op);
            oos.flush();
            MultiOperationCtx ctx = this.parallelServiceReplica.ctxs.get(msg.request.toString());
            if (ctx != null) {
                ctx.add(msg.index, msg.resp);
                if (ctx.response.isComplete() && !ctx.finished && (ctx.interger.getAndIncrement() == 0)) {
                    ctx.finished = true;
                    ctx.request.reply = new TOMMessage(this.parallelServiceReplica.id, ctx.request.getSession(),
                            ctx.request.getSequence(), msg.resp,
                            this.parallelServiceReplica.SVController.getCurrentViewId());
                    this.parallelServiceReplica.replier.manageReply(ctx.request, msg.m);
                }
            }
        }

        public void run() {
            MessageContextPair msg = null;
            ExecutionFIFOQueue<MessageContextPair> execQueue = new ExecutionFIFOQueue<>();
            boolean threadRecoveryFinished = false;
            while (true) {

                try {
                    logger.info("Thread {}, Draining to queue. Replica queue size: {}", this.thread_id,
                            this.requests.size());
                    this.requests.drainToQueue(execQueue);
                    localC++;
                    localTotal = localTotal + execQueue.getSize();
                    logger.info("Thread {}, Drained to queue. Replica queue size: {}", this.thread_id,
                            this.requests.size());

                    do {
                        msg = execQueue.getNext();
                        HibridClassToThreads ct = this.parallelServiceReplica.scheduler.getClasses().get(msg.classId);
                        ByteArrayInputStream in = new ByteArrayInputStream(msg.request.getContent());
                        DataInputStream dis = new DataInputStream(in);
                        int cmd = dis.readInt();

                        logger.debug("Thread {} processing message with cmd {} and type {}", this.thread_id,
                                BFTMapRequestType.getOp(cmd),
                                ct.type);

                        if (cmd == BFTMapRequestType.RECOVERER) {
                            msg.resp = ((SingleExecutable) this.parallelServiceReplica.executor)
                                    .executeOrdered(msg.request.getContent(), msg.m);

                        } else if (cmd == BFTMapRequestType.RECOVERY_FINISHED) {
                            int partitionId = dis.readInt();
                            double logSizeMB = (this.log.isEmpty()) ? 0 : this.log.size() / 1000000f;

                            logger.debug(
                                    "Recovery process finished for partition {}! Log has {} operations already stored ({} MB).",
                                    partitionId, this.log.size(), logSizeMB);
                            threadRecoveryFinished = true;

                        } else if (ct.type == ClassToThreads.CONC) {
                            logger.debug("Executing concurrent operation {}", BFTMapRequestType.getOp(cmd));
                            this.executeConcurrentOperation(cmd, msg);

                        } else if ((ct.type == ClassToThreads.SYNC && ct.tIds.length == 1)) { // SYNC mas só com 1
                            if (cmd == BFTMapRequestType.CKP) {
                                logger.debug(
                                        "Thread {}, Got a checkpoint command in ClassToThreads.SYNC and threadIds.lenght = 1",
                                        this.thread_id);
                                logger.debug("Thread {} executing the checkpoint", this.thread_id);
                                checkpointer.makeCheckpoint(msg);
                                logger.debug("Thread {}, Cleaning log with {} operations", this.thread_id,
                                        this.log.size());
                                this.log.clear();
                                logger.info("Thread {}, Log cleaned, now log has {} operations", this.thread_id,
                                        this.log.size());
                            } else {
                                logger.debug("Executing sync operation {}", BFTMapRequestType.getOp(cmd));
                                this.executeSyncOperation(cmd, msg);
                            }
                        } else if (ct.type == ClassToThreads.SYNC) {
                            if (thread_id == this.parallelServiceReplica.scheduler.getMapping()
                                    .getExecutorThread(msg.classId)) {

                                this.parallelServiceReplica.scheduler.getMapping().getBarrier(msg.classId).await();
                                if (cmd == BFTMapRequestType.CKP) {
                                    checkpointer.makeCheckpoint(msg);
                                    this.log.clear();
                                } else {
                                    msg.resp = ((SingleExecutable) this.parallelServiceReplica.executor)
                                            .executeOrdered(msg.request.getContent(), msg.m);
                                    Operation op = new Operation(cmd, msg.classId, msg.request.getContent(),
                                            msg.request.getSequence());
                                    this.log.add(op);
                                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                                    ObjectOutputStream oos = new ObjectOutputStream(bos);
                                    oos.writeObject(op);
                                    oos.flush();

                                    MultiOperationCtx ctx = this.parallelServiceReplica.ctxs
                                            .get(msg.request.toString());

                                    if (ctx != null) { // recovery log messages
                                                       // have no context
                                        ctx.add(msg.index, msg.resp);
                                        if (ctx.response.isComplete() && !ctx.finished &&
                                                (ctx.interger.getAndIncrement() == 0)) {
                                            ctx.finished = true;
                                            ctx.request.reply = new TOMMessage(
                                                    this.parallelServiceReplica.id, ctx.request.getSession(),
                                                    ctx.request.getSequence(), msg.resp,
                                                    this.parallelServiceReplica.SVController.getCurrentViewId());
                                            this.parallelServiceReplica.replier.manageReply(ctx.request, msg.m);
                                        }
                                    }
                                }
                                this.parallelServiceReplica.scheduler.getMapping().getBarrier(msg.classId).await();
                            } else {
                                this.parallelServiceReplica.scheduler.getMapping().getBarrier(msg.classId).await();

                                if (cmd == BFTMapRequestType.CKP && this.parallelServiceReplica.partition) {
                                    logger.info(
                                            "Got a checkpoint command in ClassToThreads.SYNC and it's not the class executor");
                                    checkpointer.makeCheckpoint(msg);
                                    this.log.clear();
                                }
                                this.parallelServiceReplica.scheduler.getMapping().getBarrier(msg.classId).await();
                            }

                        } else if (msg.classId == ParallelMapping.CONFLICT_RECONFIGURATION) {
                            this.parallelServiceReplica.scheduler.getMapping().getReconfBarrier().await();
                            this.parallelServiceReplica.scheduler.getMapping().getReconfBarrier().await();
                        }

                        this.parallelServiceReplica.statistics.computeStatistics(thread_id, 1);
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
        Kryo kryo;

        public RecoverHandlerThread(ParallelServiceReplica parallelServiceReplica, int rc_id, byte[] state,
                int metadata, Socket client) {
            kryo = new Kryo();
            kryo.register(ArrayList.class);
            kryo.register(Operation.class);
            kryo.register(Queue.class);
            kryo.register(LinkedList.class);
            kryo.register(byte[].class);
            this.parallelServiceReplica = parallelServiceReplica;
            this.rc_id = rc_id;
            this.state = state;
            this.metadata = metadata;
            this.client = client;
            logger.info("New connection from {}", client.getInetAddress());
        }

        @Override
        public void run() {
            int op;
            ObjectInputStream is = null;
            OutputStream os = null;
            ObjectOutputStream oos = null;
            Output output = null;
            try {
                is = new ObjectInputStream(client.getInputStream());
                os = client.getOutputStream();

                while (true) {
                    op = is.readInt();
                    output = new Output(os, 1500000);
                    logger.info("Recover thread {} received a request with op {}", this.rc_id,
                            BFTMapRequestType.getOp(op));
                    if (op == BFTMapRequestType.METADATA) {
                        logger.info("Recover received a request recover operation of type: METADATA");
                        String data = "";
                        try {
                            String metadataFile;
                            if (this.parallelServiceReplica.partition) {
                                metadataFile = getPartitionPath(this.rc_id) + File.separator + "metadata" +
                                        File.separator + "map" + this.rc_id + "-metadata.txt";
                            } else {
                                metadataFile = getPartitionPath(this.rc_id) + File.separator + "metadata" +
                                        File.separator + "ALLmap-metadata.txt";
                            }
                            data = new String(Files.readAllBytes(Paths.get(metadataFile)));
                            data = data.replace("\n", "").replace("\r", "");
                            data = data.replace("#", "");
                            int x = Integer.parseInt(data);
                            this.metadata = x;
                            logger.info("Sending metadata({}) of partition {} from metadata file {}", this.metadata,
                                    this.rc_id, metadataFile);
                            kryo.writeClassAndObject(output, this.metadata);
                            // os.writeInt(this.metadata);
                            logger.info("Metadata written to the output of partition {}", this.rc_id);
                        } catch (NoSuchFileException nofile) {
                            logger.warn("No metadata file to send");
                            this.metadata = Integer.MAX_VALUE;
                            // os.writeInt(this.metadata);
                            kryo.writeClassAndObject(output, this.metadata);
                        }

                    } else if (op == BFTMapRequestType.STATE) {
                        logger.info("Recover received a request recover operation of type: STATE");
                        String cpdir, dir;
                        if (this.parallelServiceReplica.partition) {
                            cpdir = getPartitionPath(this.rc_id) + File.separator + "states" + File.separator + "map" +
                                    this.rc_id + ".ser";
                            dir = getPartitionPath(this.rc_id) + File.separator + "states" + File.separator;
                        } else {
                            cpdir = getPartitionPath(this.rc_id) + File.separator + "states" + File.separator +
                                    "ALLmap" + this.rc_id + ".ser";
                            dir = getPartitionPath(this.rc_id) + File.separator + "states" + File.separator;
                        }
                        new File(dir).mkdirs();

                        Object checkpointState = null;
                        try (ObjectInputStream ins = new ObjectInputStream(new FileInputStream(cpdir))) {
                            checkpointState = ins.readObject();
                            this.state = (byte[]) checkpointState;
                            // os.writeObject(this.state);
                            logger.info("Sending checkpoint of partition {} with {} bytes", this.rc_id,
                                    this.state.length);
                            kryo.writeClassAndObject(output, checkpointState);
                        } catch (FileNotFoundException ex) {
                            logger.warn("There is no checkpoint file to send, sending empty object");
                            checkpointState = new byte[0];
                            // os.writeObject(checkpointState);
                            kryo.writeClassAndObject(output, checkpointState);
                        } catch (IOException ex) {
                            logger.error("Failed reading local states files", ex);
                        } catch (ClassNotFoundException e) {
                            logger.error("ClassNotFoundException", e);
                        }
                    } else if (op == BFTMapRequestType.LOG) {
                        Queue<Operation> log = this.parallelServiceReplica.workers[rc_id].log;
                        logger.info("Sending log for partition {} with {} operations", this.rc_id, log.size());
                        kryo.writeClassAndObject(output, log);
                        // oos = new ObjectOutputStream(os);
                        // oos.writeObject(log);
                        // oos.flush();
                    }
                    logger.info("Flushing output for partition {}", this.rc_id);
                    // output.endChunk();
                    output.flush();
                    // os.close();
                }
            } catch (EOFException e) {
                logger.info("Nothing to read from {} for partition {}, closing socket", this.client.getInetAddress(),
                        this.rc_id);
                try {
                    is.close();
                } catch (IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                return;

            } catch (IOException ex) {
                logger.error("Failure handling recover operation", ex);
                throw new RuntimeException(ex);
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
        Kryo kryo;

        public Receiver(ParallelServiceReplica parallelServiceReplica, int id, int processId,
                ReplicaContext replicaContext) {
            this.parallelServiceReplica = parallelServiceReplica;
            this.rc_id = id;
            this.processId = processId;
            this.replicaContext = replicaContext;
            kryo = new Kryo();
            kryo.register(ArrayList.class);
            kryo.register(Operation.class);
            kryo.register(Queue.class);
            kryo.register(LinkedList.class);
            kryo.register(byte[].class);
        }

        @Override
        public void run() {

            int cid; // ultimo cid de cada particao salvo nos metadados
            logger.info("Started recovery of partition {}", this.rc_id);
            String filename = "map" + this.rc_id + ".ser";
            String dataname = "map" + this.rc_id + "-metadata.txt";

            boolean stateFound = false;

            try (FileInputStream file = new FileInputStream(getPartitionPath(this.rc_id) + File.separator + "states" +
                    File.separator + filename);
                    ObjectInputStream in = new ObjectInputStream(file)) {
                logger.info("Restoring state from local storage");
                Map<Integer, byte[]> m = new TreeMap<Integer, byte[]>();
                byte[] b = (byte[]) in.readObject();

                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                DataOutputStream dos = new DataOutputStream(bos);
                dos.writeInt(BFTMapRequestType.RECOVERER);
                dos.flush();
                oos = new ObjectOutputStream(bos);
                oos.writeInt(this.rc_id);
                oos.writeObject(m);
                oos.flush();
                bos.flush();

                TOMMessage req = new TOMMessage(this.parallelServiceReplica.id, 1, 1, bos.toByteArray(), 1);
                req.groupId = (Integer.toString(this.rc_id) + "#S").hashCode();
                this.parallelServiceReplica.scheduler.schedule(new MessageContextPair(req, req.groupId, 0, null, null));

                // ((SingleExecutable)executor).executeOrdered(bos.toByteArray(),
                // null);
                logger.info("State restored from local storage");
                stateFound = true;
            } catch (IOException | ClassNotFoundException ex) {
                logger.warn("NO local checkpoint for partition {} found", this.rc_id);
            }

            // leitura de metadados e inicialização da lista de requisições
            Map<Integer, Integer> requests = new TreeMap<Integer, Integer>();
            try (FileInputStream filet = new FileInputStream(getPartitionPath(this.rc_id) + File.separator +
                    "metadata" + File.separator + dataname);
                    BufferedReader br = new BufferedReader(new InputStreamReader(filet))) {
                logger.info("Restoring metadata from local storage");
                cid = Integer.parseInt(br.readLine());
                requests.put(this.rc_id, 2);
                logger.info("Successfully restored metadata from local storage");
                // @author: henriquedsg
                if (stateFound) { // metadata and state found locally
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
            Map<Integer, ObjectOutputStream> oss = new HashMap<Integer, ObjectOutputStream>();
            Map<Integer, InputStream> iss = new HashMap<Integer, InputStream>();
            int port;
            InetAddress add;
            for (int i = 0; i < numOfProcesses; i++) {
                if (processId != currentView.getProcesses()[i]) {
                    add = currentView.getAddress(currentView.getProcesses()[i]).getAddress();
                    port = 6666 + this.rc_id;
                    int tries = 1;
                    while (tries <= 3) {
                        try {
                            // logger.info("Connecting to {}", add);
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
                                } catch (InterruptedException iee) {
                                }
                            }
                        }
                        tries++;
                    }

                    logger.info("Socket connected to {}", sockets[i].getInetAddress());
                    // requisitando metadados
                    try {
                        oss.put(currentView.getProcesses()[i], new ObjectOutputStream(sockets[i].getOutputStream()));
                        iss.put(currentView.getProcesses()[i], sockets[i].getInputStream());

                        logger.info("Requesting metadata to {}", sockets[i].getRemoteSocketAddress());
                        oss.get(currentView.getProcesses()[i]).writeInt(BFTMapRequestType.METADATA);
                        oss.get(currentView.getProcesses()[i]).flush();

                        logger.info("Reading metadata's request reply from {}", sockets[i].getRemoteSocketAddress());
                        int lastcid = (int) kryo
                                .readClassAndObject(new Input(iss.get(currentView.getProcesses()[i]), 1500000));
                        if (lastcid == Integer.MAX_VALUE)
                            lastcid = 0;
                        logger.info("Last CID is {}", lastcid);
                        if (lastcid > cid) {
                            requests.put(this.rc_id, currentView.getProcesses()[i]);
                            cid = lastcid;
                        }

                    } catch (IOException ex) {
                        logger.error("Receiver thread failed while requesting metadata to {}",
                                sockets[i].getRemoteSocketAddress(), ex);
                        throw new RuntimeException(ex);
                    }
                }
            }

            // requisitando estado
            logger.info("Request rc_id value is {} and replica Id {}", requests.get(this.rc_id),
                    this.parallelServiceReplica.id);
            if (requests.get(this.rc_id) != this.parallelServiceReplica.id) {
                Map<Integer, Integer> classIdLogCount = new HashMap<>();
                ObjectOutputStream os2 = oss.get(requests.get(this.rc_id));

                try {
                    logger.info("Requesting checkpoint state to {} of partition {}", requests.get(this.rc_id),
                            this.rc_id);
                    os2.writeInt(BFTMapRequestType.STATE);
                    os2.flush();

                    ByteArrayOutputStream bos;
                    DataOutputStream dos;

                    Object stateObj = kryo.readClassAndObject(new Input(iss.get(requests.get(this.rc_id)), 1500000));
                    byte[] state = (byte[]) stateObj;
                    if (state.length == 0) {
                        logger.info("Received an empty checkpoint for partition {}, ignoring recovery process",
                                this.rc_id);
                    } else {
                        logger.info("Received the checkpoint of partition {} with size {} MB from {}", this.rc_id,
                                state.length / 1000000f, requests.get(this.rc_id));

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
                        MessageContextPair msg = new MessageContextPair(
                                req, req.groupId, 0, null,
                                new MessageContext(cid, cid, TOMMessageType.REPLY, cid,
                                        this.parallelServiceReplica.scheduled, rc_id, rc_id, state, cid, cid,
                                        cid, rc_id, cid, cid, null, req, this.parallelServiceReplica.partition));
                        this.parallelServiceReplica.scheduler.schedule(msg);
                        logger.info("Scheduled checkpoint installation of partition {}", this.rc_id);

                        os2.reset();
                        logger.info("Requesting log of partition {}", this.rc_id);
                        os2.writeInt(BFTMapRequestType.LOG);
                        os2.flush();

                        logger.info("Receiving log for partition {}", this.rc_id);
                        Queue<Operation> log = (Queue<Operation>) kryo.readClassAndObject(
                                new Input(iss.get(requests.get(this.rc_id)), 1500000));

                        // ObjectInputStream ois = new
                        // ObjectInputStream(iss.get(requests.get(this.rc_id)));
                        // Queue<Operation> log =
                        // (Queue<Operation>)ois.readObject();

                        logger.info("Received log of partition {} with {} operations ({} MB)", this.rc_id, log.size(),
                                log.size() / 1000000f);

                        for (Operation o : log) {
                            TOMMessage message = new TOMMessage(this.parallelServiceReplica.id, 1, o.sequence,
                                    o.content, 1);
                            msg = new MessageContextPair(message, o.classID, 0, null, null);

                            int threadId = this.parallelServiceReplica.scheduler.getMapping()
                                    .getExecutorThread(msg.classId);
                            ((FIFOQueue) this.parallelServiceReplica.scheduler.getMapping().getAllQueues()[threadId])
                                    .add(msg);
                            logger.debug("Adding to queue of partition {}", threadId);
                            classIdLogCount.put(threadId, classIdLogCount.getOrDefault(threadId, 0) + 1);
                        }
                        logger.info("Added messages to the respective queues:");
                        for (Entry<Integer, Integer> kv : classIdLogCount.entrySet()) {
                            logger.info("Added {} logs to thread {}", kv.getValue(), kv.getKey());
                        }
                        logger.info("Schedule log installation of partition {}", this.rc_id);
                        bos = new ByteArrayOutputStream();
                        dos = new DataOutputStream(bos);
                        dos.writeInt(BFTMapRequestType.RECOVERY_FINISHED);
                        dos.writeInt(this.rc_id);
                        dos.flush();
                        bos.flush();

                        req = new TOMMessage(this.parallelServiceReplica.getId(), 1, 1, bos.toByteArray(), 1);
                        req.groupId = (Integer.toString(this.rc_id) + "#").hashCode();

                        ((ParallelServiceReplica) this.parallelServiceReplica).scheduler
                                .schedule(new MessageContextPair(req, req.groupId, 0, null, null));
                        logger.info("Scheduled recovery finished message for partition {}.", this.rc_id);
                    }
                } catch (Exception ex) {
                    logger.warn("Failed requesting state to another replica for partition {}.", this.rc_id, ex);
                    throw new RuntimeException(ex);
                } finally {
                    try {
                        for (Map.Entry<Integer, ObjectOutputStream> entry : oss.entrySet())
                            entry.getValue().close();
                        // for (Map.Entry<Integer, ObjectInputStream> entry:
                        // iss.entrySet()) entry.getValue().close();
                    } catch (Exception ex) {
                        logger.error("Error closing sockets");
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

    class Checkpointer {

        private final Logger logger = LoggerFactory.getLogger(Checkpointer.class);
        private final ParallelServiceReplica parallelServiceReplica;
        private int cp_id;

        public Checkpointer(ParallelServiceReplica parallelServiceReplica, int id) {
            this.parallelServiceReplica = parallelServiceReplica;
            this.cp_id = id;
        }

        public void makeCheckpoint(MessageContextPair req) {
            logger.info("Checkpoint thread {} initialized, using partitions = {}", this.cp_id,
                    this.parallelServiceReplica.partition);
            logger.info("Initializing checkpointing procedure");
            byte[] b;
            int cid = 0;
            String filename;
            String dataname;
            ByteArrayInputStream in = new ByteArrayInputStream(req.request.getContent());
            try {
                int n = new DataInputStream(in).readInt();
                String s = new DataInputStream(in).readUTF();
                cid = new DataInputStream(in).readInt();
            } catch (IOException ex) {
                logger.error("Checkpointer failed while reading input streams", ex);
                System.exit(-1);
            }

            if (this.parallelServiceReplica.partition) {

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out);
                String s = Integer.toString(this.cp_id);
                try {
                    dos.writeInt(BFTMapRequestType.CKP);
                    dos.writeUTF(s);
                    dos.flush();
                } catch (IOException ex) {
                    logger.error("Checkpointer failed while writing to output streams", ex);
                }

                // b = ((SingleExecutable) this.parallelServiceReplica.executor)
                // .executeOrdered(out.toByteArray(), req.m);
                b = ((BFTMapServerMP) this.parallelServiceReplica.executor).getSnapshot(new int[] { this.cp_id });
                filename = "map" + this.cp_id + ".ser";
                dataname = "map" + this.cp_id + "-metadata.txt";
                // logger.info("array size = "+req.length);

                try {
                    FileOutputStream fileOut =
                            // new
                            // FileOutputStream(File.separator+"recovery"+File.separator+"states"+File.separator+filename);
                            new FileOutputStream(getPartitionPath(this.cp_id) + File.separator + "states" +
                                    File.separator + filename);

                    ObjectOutputStream out1 = new ObjectOutputStream(fileOut);
                    out1.writeObject(b);
                    out1.flush();
                    out1.close();
                    fileOut.close();
                    BufferedWriter writer =
                            // new BufferedWriter(new
                            // FileWriter(File.separator+"recovery"+File.separator+"metadata"+File.separator+dataname));
                            new BufferedWriter(
                                    new FileWriter(getPartitionPath(this.cp_id) + File.separator +
                                            "metadata" + File.separator + dataname));

                    writer.write(Integer.toString(cid));
                    writer.write("\n#");
                    writer.close();
                } catch (Exception ex) {
                    logger.error("Error trying to write checkpoint", ex);
                    System.exit(-1);
                }

            } else {
                // b = ((SingleExecutable) this.parallelServiceReplica.executor)
                // .executeOrdered(req.request.getContent(), req.m);
                b = ((BFTMapServerMP) this.parallelServiceReplica.executor).getSnapshot(new int[] { this.cp_id });
                filename = "ALLmap" + this.cp_id + ".ser";
                dataname = "ALLmap-metadata.txt";

                try (
                        FileOutputStream fileOut =
                                // new
                                // FileOutputStream(File.separator+"recovery"+File.separator+"states"+File.separator+filename);
                                new FileOutputStream(
                                        getPartitionPath(this.cp_id) + File.separator + "states" +
                                                File.separator + filename);
                        ObjectOutputStream out1 = new ObjectOutputStream(fileOut);
                        BufferedWriter writer =
                                // new BufferedWriter(new
                                // FileWriter(File.separator+"recovery"+File.separator+"metadata"+File.separator+dataname));
                                new BufferedWriter(
                                        new FileWriter(getPartitionPath(this.cp_id) + File.separator +
                                                "metadata" + File.separator + dataname))) {

                    out1.writeObject(b);
                    out1.flush();

                    writer.write(Integer.toString(cid));
                    writer.write("\n#");
                } catch (Exception ex) {
                    logger.error("Error trying to write checkpoint", ex);
                    System.exit(-1);
                }
            }
            logger.info("Checkpointing has finished");
        }
    }
}
