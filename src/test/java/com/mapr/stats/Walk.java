package com.mapr.stats;

import com.google.common.collect.Lists;

import java.util.List;

public class Walk {
  public static void main(String[] args) {
    List<BetaWalk> x = Lists.newArrayList();
    for (int i = 0; i < 30; i++) {
      x.add(new BetaWalk(1, 40, 0.001));
    }
    double[] p = new double[x.size()];

    for (long j = 0; j < 20000; j++) {
      for (int i = 0; i < 30; i++) {
        p[i] = x.get(i).step();
      }

      print(j, p);
    }
  }

  private static void print(long step, double[] p) {
    System.out.printf("%d", step);
    for (int i = 0; i < 30; i++) {
      System.out.printf("\t%.8f", p[i]);
    }
    System.out.printf("\n");
  }
}
