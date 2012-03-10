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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Formatter;
import java.util.List;
import java.util.Set;

/**
 * Factory methods for mocked objects including Tuples.
 */
@SuppressWarnings("UnusedParameters")
public class Fake {

  private static final long[] time = {1235L};
  private static final Clock adjuster = new Clock();

  /**
   * Mocks up a tuple that has specially injected data.
   *
   * @param data  The data that should be in the tuple in alternating key/value form.
   * @return The mocked tuple containing the specified data.
   */
  public static Tuple tuple(Object... data) {
    final List<String> fields = Lists.newArrayList();
    final List<Object> values = Lists.newArrayList();
    for (int i = 0; i < data.length; i += 2) {
      fields.add((String) data[i]);
      values.add(data[i + 1]);
    }

    new MockUp<Tuple>() {
      FakeTuple it;

      @Mock
      public void $init(TopologyContext context, List<Object> values, int taskId, String streamId) throws IllegalAccessException, NoSuchFieldException {
        Field f = Tuple.class.getDeclaredField("values");
        f.setAccessible(true);
        f.set(it, values);
      }

      @Mock
      public Fields getFields() {
        return it.fields;
      }
    };

    return new FakeTuple(fields, values);
  }
  
  public static class FakeTuple extends Tuple {
    final Fields fields;

    public FakeTuple(List<String> fields, List<Object> values) {
      super(null, values, 0, null);
      this.fields = new Fields(fields);
    }

    @Override
    public String toString() {
      int i = 0;
      Formatter f = new Formatter();
      f.format("[");
      String separator = "";
      for (String field : fields) {
        f.format("%s%s=%s", separator, field, getValue(i++));
        separator = ", ";
      }
      f.format("]");
      return f.toString();
    }
  }

  /**
   * Mocks up an output collector that remembers what it receives.
   *
   * @return a newly allocated mock object that fills state variables.
   */
  public static OutputCollector collector(final List<AnchoredTuple> output, final Set<Tuple> acknowledgements, final Set<Tuple> failures) {
    new MockUp<FakeOutputCollector>() {
      public FakeOutputCollector it;

      @Mock
      public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        it.output.add(new AnchoredTuple(anchors, tuple));
        return null;
      }

      @Mock
      public void ack(Tuple input) {
        it.acknowledgements.add(input);
      }

      @Mock
      public void fail(Tuple tuple) {
        it.failures.add(tuple);
      }
    };

    return new FakeOutputCollector(output, acknowledgements, failures);
  }

  private static class FakeOutputCollector extends OutputCollector {
    final List<AnchoredTuple> output;
    final Set<Tuple> acknowledgements;
    private final Set<Tuple> failures;

    private FakeOutputCollector(List<AnchoredTuple> output, Set<Tuple> acknowledgements, Set<Tuple> failures) {
      this.output = output;
      this.acknowledgements = acknowledgements;
      this.failures = failures;
    }

    /**
     * Returns the task ids that received the tuples.
     */
    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
      output.add(new AnchoredTuple(anchors, tuple));
      return null;
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
      throw new UnsupportedOperationException("Default operation");
    }

    @Override
    public void ack(Tuple input) {
      throw new UnsupportedOperationException("Default operation");
    }

    @Override
    public void fail(Tuple input) {
      throw new UnsupportedOperationException("Default operation");
    }

    @Override
    public void reportError(Throwable error) {
      throw new UnsupportedOperationException("Default operation");
    }
  }

  public static class AnchoredTuple {
    private final Collection<Tuple> anchors;
    private final List<Object> tuple;

    public AnchoredTuple(Collection<Tuple> anchors, List<Object> tuple) {
      this.anchors = anchors;
      this.tuple = tuple;
    }

    public Collection<Tuple> getAnchors() {
      return anchors;
    }

    public List<Object> getTuple() {
      return tuple;
    }
  }

  public static Clock clock() {
    new MockUp<System>() {
      @Mock
      public long nanoTime() {
        return time[0] * 1000000;
      }
    };
    return adjuster;
  }

  public static class Clock {
    public Clock() {
    }

    public void advance(long delta) {
      time[0] += delta;
    }
  }
}
