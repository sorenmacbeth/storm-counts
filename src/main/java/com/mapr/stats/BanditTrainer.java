package com.mapr.stats;

import org.apache.mahout.math.stats.OnlineSummarizer;

import java.util.Random;

/**
 * Simulate a two-armed bandit playing against a beta-Bayesian model.
 *
 * The output indicates the quantiles of the distribution for regret relative to
 * the optimal pick.  The regret distribution is estimated by picking two random
 * conversion probabilities and then running the beta-Bayesian model for a number
 * of steps.  The regret is computed by taking the expectation for the optimal
 * choice and subtracting from the actual percentage of conversion achieved.  On
 * average, this should be somewhat negative since the model has to spend some
 * effort examining the sub-optimal choice.  The median, 25 and 75%-ile marks all
 * scale downward fairly precisely with the square root of the number of trials
 * which is to be expected from theoretical considerations.
 *
 * The beta-Bayesian model works by keeping an estimate of the posterior distribution
 * for the conversion probability for each of the bandits.  We take a uniform distribution
 * as the prior so the posterior is a beta distribution.  The model samples probabilities
 * from the two posterior distributions and chooses the model whose sample is larger.
 * As data is collected for the two bandits, the better of the bandits will quickly have
 * a pretty narrow posterior distribution and the lesser bandit will rarely have a sampled
 * probability higher than the better bandit.  This means that we will stop getting data
 * from the less bandit, but only when there is essentially no chance that it is better.
 */
public class BanditTrainer {
  public static void main(String[] args) {
    Random gen = new Random();
    System.out.printf("n\tq0\tq1\tq2\tq3\tq4\tmean\n");
    for (int n : new int[]{20, 50, 100, 200, 500, 1000, 2000, 5000}) {
      OnlineSummarizer summary = new OnlineSummarizer();
      for (int j = 0; j < 1000; j++) {
        double[] p = {
          gen.nextDouble(), gen.nextDouble()
        };
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
      System.out.printf("%d\t", n);
      for (int quartile = 0; quartile <= 4; quartile++) {
        System.out.printf("%.3f\t", summary.getQuartile(quartile));
      }
      System.out.printf("%.3f\n", summary.getMean());
    }
  }
}
