package Vdb;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.Arrays;
import java.util.Vector;

import Utils.*;

/**
 * Kstat related functionality
 */
public class NfsStats
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  private static NfsV3 nfs3_old   = new NfsV3();
  private static NfsV3 nfs3_new   = new NfsV3();
  private static NfsV3 nfs3_delta = new NfsV3();

  private static NfsV4 nfs4_old   = new NfsV4();
  private static NfsV4 nfs4_new   = new NfsV4();
  private static NfsV4 nfs4_delta = new NfsV4();

  private static long  kstat_ctl_t = 0;

  private static boolean nfs_reports_needed = false;

  public static String warning =
  "\n\nBe very clearly aware that this report shows nfsstat data for ALL \n" +
  "NFS mounted file systems on your system, and not only your test files.\n" +
  "If your Vdbench output directory is on an NFS mounted directory       \n" +
  "NFS activity generated by Vdbench reporting is also included.         \n" +
  "If you don't want Vdbench reporting included, specify a Vdbench       \n" +
  "output directory that does not use NFS.                             \n\n";



  public static void setNfsReportsNeeded(boolean bool)
  {
    nfs_reports_needed = bool;
  }
  public static boolean areNfsReportsNeeded()
  {
    return nfs_reports_needed;
  }

  /**
   * Obtain kstat statistics for all three NFS versions.
   *
   * Note: I think it is possible that when this code is called and auto nfs
   * mount has just beenm done or not done yet, the Kstat instance may not exist
   * yet.
   */
  public static void getAllNfsDeltasFromKstat()
  {
    String data;
    long tod = Native.get_simple_tod();

    if (kstat_ctl_t == 0)
    {
      /* Create Kstat structures: */
      kstat_ctl_t = NamedKstat.kstat_open();

      /* Check to see if we can get data: */
      data = NamedKstat.kstat_lookup_stuff(kstat_ctl_t, "nfs", "rfsreqcnt_v3");
      if (data.startsWith("JNI"))
      {
        common.ptod("getAllNfsStats(): no data found for nfs version3");
        nfs3_delta = null;
      }

      data = NamedKstat.kstat_lookup_stuff(kstat_ctl_t, "nfs", "rfsreqcnt_v4");
      if (data.startsWith("JNI"))
      {
        common.ptod("getAllNfsStats(): no data found for nfs version4");
        nfs4_delta = null;
      }
    }

    if (nfs3_delta != null)
    {
      data = NamedKstat.kstat_lookup_stuff(kstat_ctl_t, "nfs", "rfsreqcnt_v3");
      nfs3_new = new NfsV3();
      nfs3_new.parseNamedData(data);
      nfs3_new.setTime(tod);
    }

    if (nfs4_delta != null)
    {
      data = NamedKstat.kstat_lookup_stuff(kstat_ctl_t, "nfs", "rfsreqcnt_v4");
      nfs4_new = new NfsV4();
      nfs4_new.parseNamedData(data);
      nfs4_new.setTime(tod);
    }

    allDelta();
    newToOld();

    //common.ptod("nfs3_delta.getTotal(): " + nfs3_delta.getTotal());
  }

  public static void newToOld()
  {
    nfs3_old = nfs3_new;
    nfs4_old = nfs4_new;
  }

  public static void allDelta()
  {
    if (nfs3_delta != null) nfs3_delta.delta(nfs3_new, nfs3_old);
    if (nfs4_delta != null) nfs4_delta.delta(nfs4_new, nfs4_old);
  }


  /**
   * Open NFS reports
   */
  public static void createNfsReports()
  {
    /* Look at all hosts: */
    for (int i = 0; i < Host.getDefinedHosts().size(); i++)
    {
      Host host = (Host) Host.getDefinedHosts().elementAt(i);
      if (host.getHostInfo() != null)
        host.createNfsReports();
    }
  }

  /**
   * Create reporting layout for one NFS type.
   */
  public static reporting[] nfsLayout(NamedData   nameddata,
                                      Object      owner)
  {
    String[] titles = nameddata.getFieldTitles();

    /* Allocate an array for headers. */
    /* This includes one extra for 'interval', and one last one for 'null=end': */
    reporting[] rep = new reporting[titles.length + 2];

    rep[0] = new reporting("interval", "", "%*s", 8);

    for (int i = 0; i < titles.length; i++)
    {
      String title = titles[i];
      int width = Math.max(5, title.length());
      rep[i+1] = new reporting(title, "rate", "%*.1F", width);
    }

    return rep;
  }



  public static void PrintAllNfs(String title)
  {
    for (int i = 0; i < Host.getDefinedHosts().size(); i++)
    {
      Host host = (Host) Host.getDefinedHosts().elementAt(i);
      if (host.getHostInfo() != null)
        host.PrintNfsstatInterval(title);
    }
  }


  public static void NfsPrint(Report      report,
                              NamedData   nameddata,
                              reporting[] rep,
                              String      title,
                              Object      owner)
  {
    StringBuffer buf = new StringBuffer(256);

    buf.append(rep[0].report( title));

    String[] labels = nameddata.getFieldTitles();

    for (int i = 0; i < labels.length; i++)
    {
      buf.append(rep[i+1].report(nameddata.getRate(i)));
    }

    report.println(common.tod() + buf.toString());
  }

  /**
   * Vertical 'end' report for total statistics:
   */
  public static void NfsPrintVertical(Report      report,
                                      NamedData   nameddata)
  {
    String[] labels = nameddata.getFieldTitles();

    // No sort unless we change .getRate(i) call below!
    //Arrays.sort(labels);

    /* Create total: */
    double total_rate = 0;
    for (int i = 0; i < labels.length; i++)
      total_rate += nameddata.getRate(i);

    if (total_rate == 0)
      return;

    /* Report individuals: */
    report.println("");
    report.println("Non-zero rates. Note that the %% is NOT expected to represent Vdbench skew:");
    for (int i = 0; i < labels.length; i++)
    {
      double rate = nameddata.getRate(i);

      /* Including %% gets confusing since they have NO relation to Operation count, */
      /* but it is still useful. */
      if (rate > 0)
        report.println("%s %-12s %,8.0f %5.2f%%", common.tod(), labels[i], rate,
                       rate * 100. / total_rate);
    }

    report.println("%s %-12s %,8.0f", common.tod(), "Total", total_rate);
  }



  public static NfsV3 getNfs3()
  {
    return nfs3_delta;
  }
  public static NfsV4 getNfs4()
  {
    return nfs4_delta;
  }
}


