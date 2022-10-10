package demo.bftmap;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class Blacklist {

    private final transient Logger logger = LoggerFactory.getLogger(Blacklist.class);
    private Set<Integer> blacklist = new HashSet<Integer>();
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public Blacklist() {
        List<InetSocketAddress> addresses = new ArrayList<>();
        try {
            List<String> lines = Files.readAllLines(Paths.get("config/hosts.config"));
            for (String line : lines) {
                if (line.startsWith("#") || line.trim().isEmpty())
                    continue;

                String[] parts = line.split(" ");
                //String id = parts[0];
                String host = parts[1];
                Integer port = 11108;
                addresses.add(new InetSocketAddress(host, port));
            }
        } catch (Exception e) {
            System.err.println("Error trying to read hosts file: " + e.getMessage());
            System.exit(0);
        }
        executorService.submit(new BlacklistObserver(addresses));
    }

    public boolean contains(int i) {
        try {
            lock.readLock().lock();
            return blacklist.contains(i);
        } finally {
            lock.readLock().unlock();
        }
    }

    class BlacklistObserver implements Runnable {
        private Socket socket;

        public BlacklistObserver(List<InetSocketAddress> addresses) {
            try {
                InetSocketAddress address = addresses.get(0);
                socket = new Socket();                                                 //leaks everywhere
                logger.info("Connecting to: {}", address);
                socket.connect(address);
                logger.info("Connected to: {}", address);
            } catch (Exception e) {
                System.err.println("Error trying to connect sockets: " + e.getMessage());
            }
        }

        @Override
        public void run() {
            ObjectInputStream ois = null;
            try {
                ois = new ObjectInputStream(socket.getInputStream());
                while (true) {
                    logger.info("Waiting for new notification");
                    Integer partitionsSize = ois.readInt();
                    try {
                        lock.writeLock().lock();
                        logger.info("Partitions size: {}", partitionsSize);
                        Set<Integer> newBlacklist = new HashSet<>();
                        for (int i = 0; i < partitionsSize; i++) {
                            newBlacklist.add(ois.readInt());
                        }
                        blacklist = newBlacklist;
                        logger.info("Blacklist: {}", blacklist);
                    } finally {
                        lock.writeLock().unlock();
                    }
                }
            } catch (Exception e) {
                logger.error("Error trying to read/write sockets", e);
                System.exit(0);
            } finally {
                try {
                    ois.close();
                } catch (Exception e) {
                }
            }
        }
    }
}
