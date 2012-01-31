package com.mapr.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import org.junit.Test;

import java.io.FileNotFoundException;

public class SnappedCounterTest {
  @Test
  public void testShort() throws FileNotFoundException, InterruptedException {
    TopologyBuilder builder = new TopologyBuilder();

    final EventSpout spout = new EventSpout(100);
    builder.setSpout("word", spout, 5);
    builder.setBolt("count", new SnappedCounter(100, 500), 3)
      .shuffleGrouping("word");
    builder.setBolt("print", new FileBolt("foo-"), 4)
      .allGrouping("count");

    Config conf = new Config();
    conf.setDebug(true);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", conf, builder.createTopology());
    Thread.sleep(3000);
    cluster.killTopology("test");
    cluster.shutdown();
  }
}
