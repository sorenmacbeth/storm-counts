package com.mapr.storm;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TimeLimitedJoinTest {
  @Test
  public void testJoinWithExpiration() throws InterruptedException {
    TimeLimitedJoin tlj = new TimeLimitedJoin(1000, 100, new Fields("key1", "key2"));
    final FakeClock clock = new FakeClock();
    tlj.setClock(clock);

    List<Fake.AnchoredTuple> out = new ArrayList<Fake.AnchoredTuple>();
    Set<Tuple> acks = Sets.newHashSet();
    Set<Tuple> failures = Sets.newHashSet();
    tlj.prepare(null, null, Fake.collector(out, acks, failures));

    // t1 and t3 match as do t2 and t4
    Tuple t1 = Fake.tuple("key1", "k1", "key2", "k2", "v", "value1.match");
    Tuple t2 = Fake.tuple("key1", "x1", "key2", "x2", "v", "value1.nomatch");
    Tuple t3 = Fake.tuple("key1", "k1", "key2", "k2", "v", "value2.match");
    Tuple t4 = Fake.tuple("key1", "x1", "key2", "x2", "v", "value2.nomatch");
    
    tlj.execute(t1);
    clock.advance(200);

    // t2 is within the match window
    tlj.execute(t2);
    
    tlj.execute(t3);
    
    // t4 arrives too late to match t3
    clock.advance(2000);
    tlj.execute(t4);


    assertEquals(1, out.size());
    Fake.AnchoredTuple r = out.get(0);
    assertTrue(Sets.newHashSet(r.getAnchors()).containsAll(ImmutableList.of(t1, t3)));
    assertEquals("[[k1, k2], [key1=k1, key2=k2, v=value1.match], [key1=k1, key2=k2, v=value2.match]]", r.getTuple().toString());

    assertTrue(acks.containsAll(ImmutableList.of(t1, t2, t3)));
    assertEquals(0, failures.size());
    
  }

  @Test
  public void testJoinWithResurrection() throws InterruptedException {
    TimeLimitedJoin tlj = new TimeLimitedJoin(1000, 100, new Fields("key1", "key2"));
    final FakeClock clock = new FakeClock();
    tlj.setClock(clock);

    List<Fake.AnchoredTuple> out = new ArrayList<Fake.AnchoredTuple>();
    Set<Tuple> acks = Sets.newHashSet();
    Set<Tuple> failures = Sets.newHashSet();
    tlj.prepare(null, null, Fake.collector(out, acks, failures));

    // t1 and t3 match as do t2, t4 and t5
    Tuple t1 = Fake.tuple("key1", "k1", "key2", "k2", "v", "value1.match");
    Tuple t2 = Fake.tuple("key1", "x1", "key2", "x2", "v", "value1.nomatch");
    Tuple t3 = Fake.tuple("key1", "k1", "key2", "k2", "v", "value2.match");
    Tuple t4 = Fake.tuple("key1", "x1", "key2", "x2", "v", "value2.nomatch");
    Tuple t5 = Fake.tuple("key1", "x1", "key2", "x2", "v", "value3.match");

    tlj.execute(t1);
    clock.advance(200);

    // t2 is within the match window
    tlj.execute(t2);

    tlj.execute(t3);

    // t4 arrives too late to match t3
    clock.advance(2000);
    tlj.execute(t4);

    // but t5 arrives in time to match t4
    clock.advance(200);
    tlj.execute(t5);

    assertEquals(2, out.size());
    Fake.AnchoredTuple r = out.get(0);
    assertTrue(Sets.newHashSet(r.getAnchors()).containsAll(ImmutableList.of(t1, t3)));
    assertEquals("[[k1, k2], [key1=k1, key2=k2, v=value1.match], [key1=k1, key2=k2, v=value2.match]]", r.getTuple().toString());

    r = out.get(1);
    assertTrue(Sets.newHashSet(r.getAnchors()).containsAll(ImmutableList.of(t4, t5)));
    assertEquals("[[x1, x2], [key1=x1, key2=x2, v=value2.nomatch], [key1=x1, key2=x2, v=value3.match]]", r.getTuple().toString());

    assertTrue(acks.containsAll(ImmutableList.of(t1, t2, t3, t4)));
    assertEquals(0, failures.size());
  }

  private static class FakeClock implements TimeLimitedJoin.Clock, Serializable {
    long time;
    @Override
    public long now() {
      return time;
    }
    
    public void advance(long delta) {
      time += delta;
    }
  }
}
