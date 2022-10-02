package demo.bftmap;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class BlackList {

  private static final transient Logger logger = LoggerFactory.getLogger(BlackList.class);
  private Set<Integer> blacklist = new HashSet<Integer>();
  private SortedMap<Long, Integer> blacklistTimes = new TreeMap<Long, Integer>();

  private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

  public BlackList() {
     executorService.scheduleAtFixedRate(new BlacklistRemover(), 0, 1, TimeUnit.SECONDS);
  }

  public void add(int i) {
      if (blacklist.contains(i)) {
        logger.info("Blacklist contains {}", i);
        return;
      }

      logger.info("Blacklisting table {}", i);
      blacklist.add(i);
      blacklistTimes.put(System.currentTimeMillis(), i);
  }

  public boolean isBlacklisted(int i) {
      return blacklist.contains(i);
  }

  class BlacklistRemover implements Runnable {
     @Override
     public void run() {
        long now = System.currentTimeMillis();
        long allowedOffset = now - 5000;
        long key = 0;
        while (!blacklistTimes.isEmpty() && blacklistTimes.firstKey() < allowedOffset) {
          key = blacklistTimes.firstKey();
          Integer value = blacklistTimes.remove(key);
          logger.info("Removing table {} from blacklist", value);
          blacklist.remove(value);
        }
    }
  }
}
