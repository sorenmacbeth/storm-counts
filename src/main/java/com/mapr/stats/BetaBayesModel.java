package com.mapr.stats;

import org.uncommons.maths.random.MersenneTwisterRNG;

import java.util.Random;

/**
 * Two armed bandit problem where each probability is modeled by a beta prior and data about
 * positive and negative trials.  An arm is selected by sampling from the current posterior
 * for each arm and picking the one with higher sampled probability.
 */
public class BetaBayesModel {
  Random gen = new MersenneTwisterRNG();

  // we have one beta distribution for each bandit
  BetaDistribution[] b = {
    new BetaDistribution(1, 1, gen),
    new BetaDistribution(1, 1, gen)
  };
  private final double rate = 1;

  /**
   * Samples probability estimates from each bandit and picks the apparent best
   * @return 0 or 1 according to which bandit seems better
   */
  int sample(){
    double p1 = b[0].nextDouble();
    double p2 = b[1].nextDouble();
    return p1 > p2 ? 0 : 1;
  }

  /**
   * Apply feedback to the bandit we chose.
   * @param bandit      Which bandit got the impression
   * @param success     Did it pay off?
   */
  void train(int bandit, boolean success) {
    if (success) {
      b[bandit].setAlpha(b[bandit].getAlpha() + rate);
    } else {
      b[bandit].setBeta(b[bandit].getBeta() + rate);
    }
  }

  @Override
  public String toString() {
    return String.format("%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f",
      b[0].mean(), b[1].mean(),
      b[0].getAlpha(), b[0].getBeta(),
      b[1].getAlpha(), b[1].getBeta());
  }
}
