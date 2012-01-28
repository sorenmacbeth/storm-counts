package com.mapr.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Random;

/**
 * Created by IntelliJ IDEA. User: tdunning Date: 1/27/12 Time: 9:01 PM To change this template use
 * File | Settings | File Templates.
 */
public class EventSpout implements IRichSpout {
  private static Logger LOG = Logger.getLogger(EventSpout.class);

  boolean isDistributed;
  SpoutOutputCollector collector;

  public EventSpout() {
    this(true);
  }

  public EventSpout(boolean isDistributed) {
    this.isDistributed = isDistributed;
  }

  public boolean isDistributed() {
    return isDistributed;
  }

  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.collector = collector;
  }

  public void close() {

  }

  final String[] keys = new String[]{"z1", "z2", "z3", "z4", "z5"};
  final String[] words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
  final Random rand = new Random();

  public void nextTuple() {
    Utils.sleep(100);
    final Values tuple = new Values(keys[rand.nextInt(keys.length)], words[rand.nextInt(words.length)]);
    collector.emit(tuple);
    collector.emit(new Values(keys[rand.nextInt(keys.length)], words[rand.nextInt(words.length)]));
  }

  public void ack(Object msgId) {

  }

  public void fail(Object msgId) {

  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("key", "word"));
  }
}
