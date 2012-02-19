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

package com.mapr.stats;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.uncommons.maths.random.MersenneTwisterRNG;

import java.util.List;
import java.util.Random;

/**
 * Two armed bandit problem where each probability is modeled by a beta prior and data about
 * positive and negative trials.  An arm is selected by sampling from the current posterior
 * for each arm and picking the one with higher sampled probability.
 */
public class BetaBayesModel {
  Random gen = new MersenneTwisterRNG();

  // we have one beta distribution for each bandit
  List<BetaDistribution> b = Lists.newArrayList();
  private final double rate;

  public BetaBayesModel() {
    this(2, 1);
  }

  public BetaBayesModel(int bandits, double rate) {
    this.rate = rate;
    for (int i = 0; i < bandits; i++) {
      b.add(new BetaDistribution(1, 1, gen));
    }
  }

  /**
   * Samples probability estimates from each bandit and picks the apparent best
   * @return 0 or 1 according to which bandit seems better
   */
  int sample() {
    double max = 0;
    int r = 0;
    int i = 0;
    for (BetaDistribution dist : b) {
      double p = dist.nextDouble();
      if (p > max) {
        r = i;
        max = p;
      }
      i++;
    }
    return r;
  }

  /**
   * Apply feedback to the bandit we chose.
   * @param bandit      Which bandit got the impression
   * @param success     Did it pay off?
   */
  void train(int bandit, boolean success) {
    if (success) {
      b.get(bandit).setAlpha(b.get(bandit).getAlpha() + rate);
    } else {
      b.get(bandit).setBeta(b.get(bandit).getBeta() + rate);
    }
  }

  @Override
  public String toString() {
    return String.format("%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f",
      b.get(0).mean(), b.get(1).mean(),
      b.get(0).getAlpha(), b.get(0).getBeta(),
      b.get(1).getAlpha(), b.get(1).getBeta());
  }
}
