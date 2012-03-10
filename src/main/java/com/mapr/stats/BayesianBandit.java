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

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Implements the common characteristics of the Bayesian Bandit.  All that is
 * missing is the specific distribution that is used to learn and to sample
 * the mean returns.
 *
 * Typically, an implementation only has to call addModelDistribution in its
 * constructor in order to specialize this class.
 *
 * This class can also be treated as an interface by non-Bayesian Bandit solvers
 * such as EpsilonGreedy.  Such classes will need to over-ride all of the methods
 * here.
 */
public abstract class BayesianBandit {
  // we have one distribution for each bandit
  private final List<AbstractBayesianDistribution> bd = Lists.newArrayList();

  /**
   * Samples probability estimates from each bandit and picks the apparent best
   * @return 0 or 1 according to which bandit seems better
   */
  public int sample() {
    double max = Double.NEGATIVE_INFINITY;
    int r = -1;
    int i = 0;
    for (AbstractBayesianDistribution dist : bd) {
      double p = dist.nextMean();
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
  public void train(int bandit, double success) {
    bd.get(bandit).add(success);
  }

  public boolean addModelDistribution(AbstractBayesianDistribution distribution) {
    return bd.add(distribution);
  }
}
