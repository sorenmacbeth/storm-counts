package com.mapr.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.io.LineReader;
import org.apache.log4j.Logger;
import sun.nio.ch.ChannelInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This Storm bolt counts things that it receives.  As items are received, they are also logged into
 * a recovery log.
 * <p/>
 * At fixed intervals, all counts are emitted and reset back to zero.  This also causes the snapshot
 * to be set to the current position in the log.  The snapshot contains nothing more than a
 * reference to the log.
 * <p/>
 * On startup, if we see one or more recovery logs and a snapshot, we look at the snapshot and read
 * items from the log starting where the snapshot indicates before accepting new items. If we see
 * logs but no snapshot, we don't need to read any logs before starting.
 * <p/>
 * All log files are named in a manner that allows them to be read in order.  Snapshots contain a
 * reference to a file and an offset so that we know where to start reading the log files.  A
 * snapshot may refer to a log that is not the latest.  If so, we need to read all logs up to the
 * latest in addition to the log specified in the snapshot.
 */
public class SnappedCounter implements IRichBolt {
  private static final transient Logger logger = Logger.getLogger(SnappedCounter.class);

  // formats home, time, component into a log file name
  private final String logFileNameFormat = "%1$s/%2$tY-%2$2tm-%2$td/%3$s-%2$tH-%2$tM-%2$tS.log";
  private final String snapFileNameFormat = "%s/%s.snap";

  private final long checkpointInterval = 10 * 1000;

  private String home = "storm-aggregator-logs";

  // we flush and acknowledge pending tuples when we have either seen maxBufferedTuples tuples
  // when logFlushInterval ms have passed.
  private final long logFlushInterval = 1 * 1000;
  private int maxBufferedTuples = 100000;

  private List<Tuple> ackables = Lists.newArrayList();

  private transient FileChannel log;

  private Map<String, Multiset<String>> counters = Maps.newHashMap();

  private OutputCollector outputCollector;
  private String componentId;

  private long lastCheckpoint = 0;
  private long lastFlush = 0;
  private long lastRecord = 0;
  private long reportingInterval = 300 * 1000;

  public SnappedCounter(String home) throws FileNotFoundException {
    this.home = home;
  }

  public SnappedCounter(String home, long reportingInterval) throws FileNotFoundException {
    this(home);
    this.reportingInterval = reportingInterval;
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
    this.componentId = topologyContext.getThisComponentId();
  }

  /**
   * The input tuple consists of a key and a value.  The key selects which counter table we need to
   * increment and the value is the value to be counted.
   *
   * @param tuple The (key,value) data to count.
   */
  @Override
  public void execute(Tuple tuple) {
    String key = tuple.getString(0);
    String value = tuple.getString(1);
    count(key, value);
    ackables.add(tuple);
    logItem(key, value);
    checkpoint(false);
    recordCounts();
  }

  /**
   * Records and then clears all pending counts
   */
  private void recordCounts() {
    long currentRecord = (System.currentTimeMillis() / reportingInterval) * reportingInterval;
    if (lastRecord == 0) {
      lastRecord = currentRecord;
    }

    if (currentRecord - lastRecord > reportingInterval) {
      // flush log and snapshot
      checkpoint(true);
      // record all keys
      for (String key : counters.keySet()) {
        final Multiset<String> table = counters.get(key);
        for (String value : table.elementSet()) {
          outputCollector.emit(new Values(lastRecord, key, value, table.count(value)));
        }
        lastRecord = currentRecord;
      }
      // nuke everything even though we will most likely just rebuild it
      counters.clear();
    }
  }

  /**
   * Counts a particular key/value
   *
   * @param key
   * @param value
   */
  private void count(String key, String value) {
    Multiset<String> table = counters.get(key);
    if (table == null) {
      table = HashMultiset.create();
      counters.put(key, table);
    }
    table.add(value);
  }

  /**
   * Logs a key/value.
   *
   * @param key   The kind of item
   * @param value Which item
   */
  private void logItem(String key, String value) {
    ByteBuffer out = ByteBuffer.allocate(10000);
    out.put(key.getBytes(Charsets.UTF_8));
    out.put((byte) '\t');
    out.put(value.getBytes(Charsets.UTF_8));
    out.put((byte) '\n');
    try {
      if (log == null) {
        new File(getLogFileName()).getParentFile().mkdirs();
        log = new FileOutputStream(getLogFileName(), true).getChannel();
      }
      out.flip();
      log.write(out);
      final long currentFlush = elapsedMilliSeconds();
      if (currentFlush - lastFlush > logFlushInterval || ackables.size() > maxBufferedTuples) {
        lastFlush = currentFlush;
        flushLog();
      }
    } catch (IOException e) {
      throw new RuntimeException("IO error in log write", e);
    }
  }

  /**
   * Record the current point in the log
   *
   * @param force Set to true to force checkpoint to be written and log flushed right now.
   */
  private void checkpoint(boolean force) {
    long currentCheckpoint = elapsedMilliSeconds();
    if (force || currentCheckpoint - lastCheckpoint > checkpointInterval) {
      lastCheckpoint = currentCheckpoint;
      File snap = snapFile();
      snap.getParentFile().mkdirs();
      try {
        Files.write(getLogFileName() + "\t" + currentLogFileOffset(), snap, Charsets.UTF_8);
        flushLog();
      } catch (IOException e) {
        throw new RuntimeException("IO error while snapping", e);
      }
    }
  }

  /**
   * Flush the log and acknowledge all pending tuples.  The idea is that we don't want to
   * acknowledge tuples until we are sure they are on disk but we also don't want to force them to
   * disk too often.
   *
   * @throws IOException
   */
  private void flushLog() throws IOException {
    for (Tuple tuple : ackables) {
      outputCollector.ack(tuple);
    }
    log.force(true);
  }

  public void replayLog() {
    File snap = snapFile();
    if (snap.exists()) {
      try {
        String snapState = Files.readFirstLine(snap, Charsets.UTF_8);
        Iterator<String> pieces = Splitter.on("\t").split(snapState).iterator();
        final String firstLog = pieces.next();
        long offset = Long.parseLong(pieces.next());
        List<File> files = Lists.newArrayList(new File(getLogFileName()).getParentFile().listFiles(new FilenameFilter() {
          @Override
          public boolean accept(File file, String name) {
            return name.compareTo(firstLog) >= 0;
          }
        }));
        Collections.sort(files);

        final Splitter onTabs = Splitter.on('\t');
        for (File file : files) {
          FileChannel in = null;
          try {
            in = new FileInputStream(file).getChannel();
            in.position(offset);
            offset = 0;
            LineReader input = new LineReader(new InputStreamReader(new ChannelInputStream(in)));
            String line = input.readLine();
            while (line != null) {
              pieces = onTabs.split(line).iterator();
              String key = pieces.next();
              count(key, pieces.next());
            }
          } catch (IOException e) {
            logger.warn("Error replaying log, log file " + file + " partially dropped", e);
          } finally {
            if (in != null) {
              Closeables.closeQuietly(in);
            }
          }
        }
      } catch (IOException e) {
        logger.warn("Error trying to read snapshot file", e);
      }
    }
  }

  @Override
  public void cleanup() {
    checkpoint(true);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("a", "b"));
  }

  public String getLogFileName() {
    return String.format(logFileNameFormat, home, (System.currentTimeMillis() / checkpointInterval) * checkpointInterval, componentId);
  }

  public void setHome(String home) {
    this.home = home;
  }

  private File snapFile() {
    return new File(String.format(snapFileNameFormat, home, System.currentTimeMillis()));
  }

  private long currentLogFileOffset() throws IOException {
    return log.position();
  }

  private long elapsedMilliSeconds() {
    return System.nanoTime() / 1000000;
  }
}
