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

import org.apache.mahout.math.stats.OnlineSummarizer;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Random;

/**
 * Simulate a two-armed bandit playing against a beta-Bayesian model.
 * <p/>
 * The output indicates the quantiles of the distribution for regret relative to the optimal pick.
 * The regret distribution is estimated by picking two random conversion probabilities and then
 * running the beta-Bayesian model for a number of steps.  The regret is computed by taking the
 * expectation for the optimal choice and subtracting from the actual percentage of conversion
 * achieved.  On average, this should be somewhat negative since the model has to spend some effort
 * examining the sub-optimal choice.  The median, 25 and 75%-ile marks all scale downward fairly
 * precisely with the square root of the number of trials which is to be expected from theoretical
 * considerations.
 * <p/>
 * The beta-Bayesian model works by keeping an estimate of the posterior distribution for the
 * conversion probability for each of the bandits.  We take a uniform distribution as the prior so
 * the posterior is a beta distribution.  The model samples probabilities from the two posterior
 * distributions and chooses the model whose sample is larger. As data is collected for the two
 * bandits, the better of the bandits will quickly have a pretty narrow posterior distribution and
 * the lesser bandit will rarely have a sampled probability higher than the better bandit.  This
 * means that we will stop getting data from the less bandit, but only when there is essentially no
 * chance that it is better.
 */
public class BanditTrainer {
  public static void main(String[] args) throws FileNotFoundException {
    System.out.printf("regret\n");
    averageRegret("regret.tsv");
    System.out.printf("error rates\n");
    errorRate("errors.tsv");
    System.out.printf("commit time\n");
    commitTime("commit.tsv", 3000, 0.1, 0.12);
    System.out.printf("done\n");
  }

  /**
   * Records which bandit was chosen for many runs of the same scenario.  This output
   * is kind of big an hard to digest visually.  As such, it is probably better to reduce
   * this using a mean.  In R, this can be done like this:
   * <pre>
   *    plot(tapply(z$k, floor(z$i/10), mean), type='l')
   * </pre>
   * @param outputFile
   * @param n
   * @param p1
   * @param p2
   * @throws FileNotFoundException
   */
  private static void commitTime(String outputFile, int n, double p1, double p2) throws FileNotFoundException {
    PrintWriter out = new PrintWriter(outputFile);
    try {
      Random gen = new Random();
      out.printf("i\tk\n");
      for (int j = 0; j < 1000; j++) {
        // pick probabilities at random
        double[] p = {
          p1, p2
        };
        Arrays.sort(p);
        BetaBayesModel s = new BetaBayesModel();
        for (int i = 0; i < n; i++) {
          int k = s.sample();
          out.printf("%d\t%d\n", i, k);

          final double u = gen.nextDouble();
          boolean r = u <= p[k];
          if (i > n / 2) {
          }
          s.train(k, r);
        }
      }
    } finally {
      out.close();
    }
  }

  /**
   * Computes error rate (the rate at which the sub-optimal choice is made as a function of the
   * two probabilities and the number of trials.  The output report contains p1, p2,
   * number-of-trials, total-correct, total-correct-in-last-half.
   *
   * The commitTime output is probably more interesting.
   *
   * @param outputFile  Where to write the data.
   */
  private static void errorRate(String outputFile) throws FileNotFoundException {
    PrintWriter out = new PrintWriter(outputFile);
    try {
      out.printf("p1\tp2\tn\twins\tlate\n");
      Random gen = new Random();
      for (int n : new int[]{20, 50, 100, 200, 500, 1000, 2000, 5000}) {
        System.out.printf("%d\n", n);
        for (int j = 0; j < 1000 * (n < 500 ? 10 : 1); j++) {
          // pick probabilities at random
          double[] p = {
            gen.nextDouble(), gen.nextDouble()
          };
          // order them to make error interpretation easier
          Arrays.sort(p);
          BetaBayesModel s = new BetaBayesModel();
          int wins = 0;
          int lateWins = 0;
          for (int i = 0; i < n; i++) {
            int k = s.sample();
            final double u = gen.nextDouble();
            boolean r = u <= p[k];
            wins += r ? 1 : 0;
            if (i > n / 2) {
              lateWins += r ? 1 : 0;
            }
            s.train(k, r);
          }
          out.printf("%.3f\t%.3f\t%d\t%d\t%d\n", p[0], p[1], n, wins, lateWins);
        }
      }
    } finally {
      out.close();
    }
  }

  /**
   * Computes average regret relative to perfect knowledge given uniform random probabilities.
   * The output contains the quartiles for different numbers of trials.  The quartiles are computed
   * by running many experiments for each specified number of trials.
   *
   * This can be plotted pretty much directly in R
   * <pre>
   * > x=read.delim(file='~/Apache/storm-aggregator/regret.tsv')
   * > bxp(list(stats=t(as.matrix(x[,2:6])), n=rep(1000,times=8),names=x$n))
   * </pre>
   * @param outputFile  Where to put the output
   * @throws FileNotFoundException If the output file can't be opened due to a missing directory.
   */
  private static void averageRegret(String outputFile) throws FileNotFoundException {
    PrintWriter out = new PrintWriter(outputFile);
    try {
      Random gen = new Random();
      out.printf("n\tq0\tq1\tq2\tq3\tq4\n");
      // for each horizon time span of interest
      for (int n : new int[]{20, 50, 100, 200, 500, 1000, 2000, 5000}) {
        System.out.printf("%d\n", n);
        OnlineSummarizer summary = new OnlineSummarizer();
        // replicate the test many times
        for (int j = 0; j < 10000; j++) {
          // pick probabilities at random
          double[] p = {
            gen.nextDouble(), gen.nextDouble()
          };
          // order them to make error interpretation easier
          Arrays.sort(p);
          BetaBayesModel s = new BetaBayesModel();
          int wins = 0;
          for (int i = 0; i < n; i++) {
            int k = s.sample();
            final double u = gen.nextDouble();
            boolean r = u <= p[k];
            wins += r ? 1 : 0;
            s.train(k, r);
          }
          summary.add((double) wins / n - Math.max(p[0], p[1]));
        }
        out.printf("%d\t", n);
        for (int quartile = 0; quartile <= 4; quartile++) {
          out.printf("%.3f%s", summary.getQuartile(quartile), quartile < 4 ? "\t" : "\n");
        }
        //      System.out.printf("%.3f\n", summary.getMean());
      }
    } finally {
      out.close();
    }
  }
}
