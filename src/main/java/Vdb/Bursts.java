package Vdb;

/*
 * Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.*;
import Utils.Format;
import Utils.Fget;

/**
 * Handle i/o rate bursts. rd=xxx,...burst=(xx,yy,xx,yy,....) - xx = iorate - yy
 * = duration in seconds
 */
public class Bursts implements Serializable, Cloneable {
  private final static String c = "Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.";

  private double[] matrix = null;
  private long[] arrivals = null;
  private double[] rates = null;
  private int ios_left_for_this_second = 0;
  private long start_time = -1000000; /* Allow one second add for the first start */
  private boolean spread = false;
  private double max_rate = 0;

  public Bursts(double[] mat, boolean spr) {

    if (mat == null)
      common.failure("'dist=variable' requested without a proper iorate= distribution list");

    spread = spr;
    matrix = (double[]) mat.clone();
    if (matrix.length % 2 == 1)
      common.failure("Burst distribution list must contain an even number " + "of values.");

    /* Count the number of seconds to check for max: */
    int seconds = 0;
    for (int i = 0; i < matrix.length; i += 2) {
      seconds += matrix[i + 1];
      max_rate = Math.max(max_rate, matrix[i]);
    }

    if (seconds > 3600)
      common.failure("Variable i/o rate distribution list may contain no more than "
          + "3600 seconds worth of i/o rates:" + seconds);
  }

  public double getMaxRate() {
    return max_rate;
  }

  public Object clone() {
    try {
      Bursts bu = (Bursts) super.clone();
      if (matrix != null)
        bu.matrix = (double[]) matrix.clone();
      if (arrivals != null)
        bu.arrivals = (long[]) arrivals.clone();

      return bu;

    } catch (Exception e) {
      common.failure(e);
    }
    return null;
  }

  public double[] getList() {
    return matrix;
  }

  public boolean isSpread() {
    return spread;
  }

  /**
   * Adjust the requested burst rates and/or interarrival times to the skew that
   * each WG_entry will run.
   */
  public void createBurstList(double skew, RD_entry rd) {
    /* First start with translating percentages to numbers: */
    for (int i = 0; i < matrix.length; i += 2) {
      if (matrix[i + 0] < 0)
        matrix[i + 0] = rd.iorate * (matrix[i + 0] * -1) / 100;
      if (matrix[i + 1] < 0) {
        double dur = (long) (rd.getElapsed() * (matrix[i + 1] * -1) / 100);
        if (dur == 0)
          common.failure("A burst duration percentage in the 'burst=' matrix "
              + "results in a duration of zero seconds: " + (matrix[i + 1] * -1) + "%.");
        matrix[i + 1] = dur;
      }
    }

    /* Count the number of seconds covered by the matrix: */
    int secs = 0;
    for (int i = 0; i < matrix.length; i += 2)
      secs += (matrix[i + 1] > 0) ? matrix[i + 1] : 1;

    if (secs > rd.getElapsed())
      common.failure(
          "The amount of time covered by 'dist=variable' iorate (%d) " + "must be shorter than elapsed time (%d)", secs,
          rd.getElapsed());

    rates = new double[secs];
    arrivals = new long[secs];
    int second = 0;

    for (int i = 0; i < matrix.length; i += 2) {
      for (int j = 0; j < matrix[i + 1]; j++) {
        rates[second] = matrix[i] * skew / 100.;
        if (matrix[i] != 0)
          arrivals[second++] = (long) (1000000. / (matrix[i] * skew / 100.));
        else
          arrivals[second++] = 0;

        // common.ptod("arrivals[ second++ ]: " + matrix[i] + " skew: " +
        // skew + " " + arrivals[ second - 1 ]);
      }
    }
  }

  public double getArrivalTime(double start_ts, int distribution) {
    if (!spread)
      return getHotArrivalTime();
    else
      return GetSpreadArrivalTime(start_ts, distribution);
  }

  /**
   * Calculate the arrival time for the current one second interval. All i/o will
   * be scheduled to start at the beginning of that one second and therefore is a
   * real hot burst!
   */
  public double getHotArrivalTime() {

    /* If we completed all the ios for the previous second, pick up the rate: */
    if (ios_left_for_this_second == 0) {
      /* Skip all zero iops seconds: */
      while (true) {
        start_time += 1000000;
        long second = (long) (start_time / 1000000.);
        ios_left_for_this_second = (int) rates[(int) (second % rates.length)];
        // common.ptod("second1: " + second + " " + ios_left_for_this_second);
        if (ios_left_for_this_second > 0)
          break;
      }
    }

    ios_left_for_this_second--;

    // long second = (long) (start_time / 1000000.);
    // if (second < 6)
    // common.ptod("second: " + second + " " + ios_left_for_this_second + " " +
    // start_time);

    return (double) start_time;
  }

  /**
   * This code spreads out the i/o over the one second interval, instead of it
   * starting all i/o at once. We may need this again.
   */
  public double GetSpreadArrivalTime(double start, int distribution) {
    double start_ts = start;
    long second = (long) start_ts / 1000000;
    double arrival = (long) arrivals[(int) (second % arrivals.length)];

    /* Skip to next second if this second we do NO i/o: */
    while (arrival == 0) {
      start_ts += 1000000;
      second = (long) start_ts / 1000000;
      arrival = (long) arrivals[(int) (second % arrivals.length)];
    }

    if (distribution == 0) {
      double delta = ownmath.exponential(arrival);
      if (delta > 180 * 1000000)
        delta = 180 * 1000000;
      start_ts += delta;
    }

    else if (distribution == 1) {
      double delta = ownmath.uniform(0, arrival * 2);
      if (delta > 1000000)
        delta = 1000000;
      start_ts += delta;
    }

    else
      start_ts += arrival;

    // common.ptod("second: " + second + " " + (int) start_ts + " " + (int) (second
    // % arrivals.length));
    return start_ts;
  }

  public static void main(String[] args) {
    int MAX = 30000000;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX; i++) {
      long tod = Native.get_simple_tod();
    }

    long end = System.currentTimeMillis();
    double seconds = (double) (end - start) / 1000.;

    common.ptod("seconds: " + seconds);
    common.ptod(Format.f("end: %.6f", ((double) MAX / seconds)));

  }
}
