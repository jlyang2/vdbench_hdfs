package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.*;
import Utils.NfsV3;
import Utils.NfsV4;

/**
 * This class contains the information that is returned back from a slave to the
 * master related to vdbench performance statistics
 */
public class SlaveStats implements Serializable {
  private final static String c = "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private long stats_number = 0;;

  private SdStats[] sd_stats = null; /* One per SD */
  private FwdStats tot_stats = null;
  private HashMap fsd_map = null;
  private HashMap fwg_map = null;
  private Kstat_cpu cpu_stats = null;

  private Vector user_data = null;

  public transient Slave owning_slave;

  /* Each element in kstat_data is in sync with InfoFromHost.instance_pointers, */
  /* and we therefore know host, lun, and instance name. */
  private Vector kstat_data = null;

  private long[] block_counters = null;

  private NfsV3 nfs3_delta = null;
  private NfsV4 nfs4_delta = null;

  public ArrayList<NwAdapter> nw_stats = null;

  public ThreadMonList tmonitor_deltas = new ThreadMonList();

  public long permit_time = 0;
  public long permit_threads = 0;

  public SlaveStats(long num) {
    stats_number = num;
    block_counters = Blocked.getCounters();

    if (SlaveJvm.isThisSlave() && common.get_debug(common.PRINT_BLOCK_COUNTERS))
      Blocked.printCountersToLog();
  }

  public void setCpuStats(Kstat_cpu ks) {
    cpu_stats = ks;
  }

  public Kstat_cpu getCpuStats() {
    return cpu_stats;
  }

  public void setSdStats(SdStats[] stats) {
    sd_stats = stats;
  }

  public SdStats[] getSdStats() {
    return sd_stats;
  }

  public long[] getBlockCounters() {
    return block_counters;
  }

  public long getNumber() {
    return stats_number;
  }

  public void setSlaveIntervalStats(FwdStats tot, HashMap fsd, HashMap fwg) {
    tot_stats = tot;
    fsd_map = fsd;
    fwg_map = fwg;
  }

  public FwdStats getSlaveIntervalStats() {
    return tot_stats;
  }

  public HashMap getFsdMap() {
    return fsd_map;
  }

  public HashMap<String, FwdStats> getFwdMap() {
    return fwg_map;
  }

  public void setKstatData(Vector ks) {
    kstat_data = ks;
  }

  public Vector getKstatData() {
    return kstat_data;
  }

  public void setNfsData(NfsV3 nfs3, NfsV4 nfs4) {
    nfs3_delta = nfs3;
    nfs4_delta = nfs4;
  }

  public NfsV3 getNfs3() {
    return nfs3_delta;
  }

  public NfsV4 getNfs4() {
    return nfs4_delta;
  }

  public void setUserData(Vector u) {
    user_data = u;
  }

  public Vector getUserData() {
    return user_data;
  }

  public void setThreadMonData(ThreadMonList data) {
    tmonitor_deltas = data;
  }

  public void printit() {
    common.ptod("stats_number: " + stats_number);
    common.ptod("sd_stats:     " + sd_stats);
    common.ptod("fwd_stats:    " + tot_stats);
    common.ptod("cpu_stats:    " + cpu_stats);
    common.ptod("kstat_data:   " + kstat_data);
    common.ptod("nfs3_delta:   " + nfs3_delta);
    common.ptod("nfs4_delta:   " + nfs4_delta);

  }
}
