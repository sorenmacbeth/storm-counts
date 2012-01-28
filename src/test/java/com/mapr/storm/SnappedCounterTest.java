package com.mapr.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;

public class SnappedCounterTest {
  @Test
  public void testShort() throws FileNotFoundException {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new EventSpout(), 10);
    builder.setBolt("count", new SnappedCounter(new File(".").getAbsolutePath()), 3)
      .shuffleGrouping("word");

    Config conf = new Config();
    conf.setDebug(true);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", conf, builder.createTopology());
    Utils.sleep(10000);
    cluster.killTopology("test");
    cluster.shutdown();
  }
}
