/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mapr.storm;

import backtype.storm.tuple.Tuple;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class CounterBoltTest {
  @Test
  public void testShort() throws FileNotFoundException, InterruptedException {
    CounterBolt cb = new CounterBolt(100, 5);

    final Fake.Clock clock = Fake.clock();

    List<Fake.AnchoredTuple> out = new ArrayList<Fake.AnchoredTuple>();
    Set<Tuple> acks = Sets.newHashSet();
    Set<Tuple> failures = Sets.newHashSet();

    cb.prepare(null, null, Fake.collector(out, acks, failures));

    Tuple t1 = Fake.tuple("key", "t1", "value", "x1");
    Tuple t2 = Fake.tuple("key", "t1", "value", "x2");
    Tuple t3 = Fake.tuple("key", "t1", "value", "x3");
    Tuple t4 = Fake.tuple("key", "t1", "value", "x4");
    Tuple t5 = Fake.tuple("key", "t1", "value", "x5");
    Tuple t6 = Fake.tuple("key", "t1", "value", "x6");
    Tuple t7 = Fake.tuple("key", "t1", "value", "x7");
    Tuple t8 = Fake.tuple("key", "t1", "value", "x8");
    cb.execute(t1);
    clock.advance(10);
    cb.execute(t1);
    clock.advance(10);
    cb.execute(t2);
    clock.advance(10);
    cb.execute(t1);
    clock.advance(10);
    cb.execute(t3);
    clock.advance(10);
    cb.execute(t3);
    clock.advance(10);
    cb.execute(t4);
    clock.advance(10);
    cb.execute(t5);
    cb.execute(t6);

    // 3 and then 2 tuples went out, one due to buffer size, one due to time
    // the last one is still buffered
    assertEquals(5, out.size());

    cb.execute(t7);
    cb.execute(t8);
    cb.execute(t7);
    clock.advance(200);
    cb.execute(t7);

    assertEquals(8, out.size());
    cb.cleanup();

    assertEquals(13, cb.getTotal());
  }
}
