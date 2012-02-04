package com.mapr.stats;

import org.apache.mahout.math.jet.random.AbstractContinousDistribution;
import org.apache.mahout.math.jet.random.Gamma;
import org.uncommons.maths.random.MersenneTwisterRNG;

import java.util.Random;

/**
 * Sample from a beta distribution.
 */
public class BetaDistribution extends AbstractContinousDistribution {
  private Gamma gAlpha;
  private Gamma gBeta;
  private double alpha, beta;

  public BetaDistribution(double alpha, double beta, Random random) {
    this.alpha = alpha;
    this.beta = beta;
    gAlpha = new Gamma(alpha, 1, random);
    gBeta = new Gamma(beta, 1, random);
  }

  public BetaDistribution(double alpha, double beta) {
    this(alpha, beta, new MersenneTwisterRNG());
  }

  /**
   * Returns a random number from the distribution.
   *
   * @return A new sample from this distribution.
   */
  @Override
  public double nextDouble() {
    double x = gAlpha.nextDouble();
    double y = gBeta.nextDouble();
    return x / (x + y);
  }

  @Override
  public double pdf(double x) {
    return Math.pow(x, alpha - 1) * Math.pow(1 - x, beta - 1) / org.apache.mahout.math.jet.stat.Gamma.beta(alpha, beta);
  }

  public double logPdf(double x) {
    return x * (alpha - 1) + (1 - x) * (beta - 1) - Math.log(org.apache.mahout.math.jet.stat.Gamma.beta(alpha, beta));
  }

  @Override
  public double cdf(double x) {
    return org.apache.mahout.math.jet.stat.Gamma.incompleteBeta(alpha, beta, x);
  }

  public void setAlpha(double alpha) {
    this.alpha = alpha;
  }

  public void setBeta(double beta) {
    this.beta = beta;
  }

  public double getBeta() {
    return beta;
  }

  public double getAlpha() {
    return alpha;
  }

  public double mean() {
    return alpha / (alpha + beta);
  }

  /**
   * Sets the uniform random generator internally used.
   *
   * @param rand the new PRNG
   */
  @Override
  public void setRandomGenerator(Random rand) {
    gAlpha.setRandomGenerator(rand);
    gBeta.setRandomGenerator(rand);
  }
}
