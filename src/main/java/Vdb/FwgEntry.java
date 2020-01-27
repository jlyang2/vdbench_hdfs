package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;
import java.io.*;
import Utils.printf;
import Utils.Format;


/**
 * This class handles all FWD workload generation data.
 * FWG: File Workload Generator.
 */
class FwgEntry implements Serializable, Comparable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private String     fwg_name;
  public  String     fsd_name;
  public  String     host_name;
  public  FileAnchor anchor;
  public  FileAnchor target_anchor;

  public  double[]   filesizes;
  public  double[]   xfersizes;

  public  boolean    select_random;
  public  boolean    selseq_start0;
  public  boolean    select_once;
  public  boolean    select_full;
  public  boolean    select_empty;
  public  boolean    select_nfull;
  public  double     poisson_skew = 0;

  public  boolean    sequential_io;
  public  boolean    seq_io_start0;
  public  boolean    del_b4_write;
  public  boolean    file_sharing;
  public  boolean    shared;
  public  long       stopafter = Long.MAX_VALUE;
  public  double     skew;
  public  int        threads;
  public  OpenFlags  open_flags;

  public  int        width;
  public  int        depth;
  public  int        files;
  public  long       total_size;
  public  long       working_set;
  public  String     dist;

  public  boolean    work_done = false;

  private Random     xfer_size_randomizer = new Random(0); /* Fixed seed! */

  public  Blocked    blocked = new Blocked();

  private boolean    shutting_down = false;

  private int        operation;
  public  double     readpct = -1;

  public transient  FwdEntry fwd_used;  /* for debuging only */

  private static int sequence = 0;
  private int seqno = sequence++;

  public boolean create_rw_log = false;

  /* This Dedup instance is temporarily held here going from FSD to FileAnchor */
  public Dedup dedup = null;



  /**
   * Create an FwgEntry() instance.
   * Operation can come from FwdEntry, or from rd=xxx,operations=(..,..) or
   * from rd=xxx,foroperation=(..,..)
   */
  public FwgEntry(FsdEntry fsd,
                  FwdEntry fwd,
                  RD_entry rd,
                  String   host_in,
                  int      requested_operation)
  {
    fwd_used       = fwd;
    fwg_name       = fwd.fwd_name;
    host_name      = host_in;
    xfersizes      = fwd.xfersizes;
    select_random  = fwd.select_random;
    select_once    = fwd.select_once;
    select_full    = fwd.select_full;
    select_empty   = fwd.select_empty;
    select_nfull   = fwd.select_nfull;
    selseq_start0  = fwd.selseq_start0;
    poisson_skew   = fwd.poisson_skew;
    sequential_io  = fwd.sequential_io;
    seq_io_start0  = fwd.seq_io_start0;
    file_sharing   = fwd.file_sharing;
    del_b4_write   = fwd.del_b4_write;
    stopafter      = fwd.stopafter;
    skew           = fwd.skew;
    threads        = fwd.threads;
    readpct        = fwd.readpct;

    fsd_name       = fsd.name;
    shared         = fsd.shared;
    filesizes      = fsd.filesizes;
    anchor         = fsd.anchor;
    width          = fsd.width;
    depth          = fsd.depth;
    files          = fsd.files;
    dist           = fsd.dist;
    working_set    = fsd.working_set;
    total_size     = fsd.total_size;
    operation      = requested_operation;
    open_flags     = fsd.open_flags;
    create_rw_log  = fsd.create_rw_log;

    dedup          = fsd.dedup;

    //if (dedup == null)
    //  common.failure("may not be null during first test");

    if (xfersizes.length == 1 && xfersizes[0] == -1)
      common.failure("No xfersize= specified for fwd=" + fwd.fwd_name);

    if (sequential_io && readpct >= 0)
      common.failure("'fileio=sequential' and 'rdpct=' are mutually exclusive.");

    /* Handle openflag overrides here: */
    if (fwd.open_flags != null)
      open_flags = fwd.open_flags;
    if (rd.open_flags != null)
      open_flags = rd.open_flags;

    /* Code added to allow target= for copy/move: */
    if (fwd.target_fsd != null)
      setTargetFsd(this, fwd);


    if (operation < 0)
      common.failure("Negative operation");

    /* A workload may run on only one host or slave. This means that wildcarding */
    /* is not useful. However, for simplicity, the default host name is '*' */
    /* which will translate to the only define hist IF there is only one host. */
    //if (host_name.equals("*") && Host.getDefinedHosts().size() == 1)
    //  host_name = ((Host) Host.getDefinedHosts().firstElement()).getLabel();

    /* If we know we'll be doing cloud stuff pass a CurlHandling instance to Fwg: */
    if (fsd.cloud_url != null && anchor.curl == null)
      anchor.curl = new CurlHandling(fsd);
  }


  public String getName()
  {
    return fwg_name;
  }

  /**
   * Get a transfer size from our distribution table.
   *
   * Note: the pct loop done below can be faster if I first create a 100 element
   * array with xfersizes 0-99!
   */
  public int getXferSize()
  {
    /* Journal recovery uses the xfersize specified in the map: */
    if (Validate.isJournalRecoveryActive())
    {
      return anchor.getDVMap().getKeyBlockSize();
    }

    if (xfersizes.length == 1)
    {
      return(int) xfersizes[0];
    }

    int pct = xfer_size_randomizer.nextInt(100);
    int cumpct = 0;
    int i;

    for (i = 0; i < xfersizes.length; i+=2)
    {
      cumpct += xfersizes[i+1];
      if (pct < cumpct)
        break;
    }

    int size = (int)  xfersizes[i];

    return size;
  }

  public int getMaxXfersize()
  {
    if (Validate.isJournalRecoveryActive())
      return anchor.getDVMap().getKeyBlockSize();

    if (xfersizes.length == 1)
      return(int) xfersizes[0];

    double max = 0;
    for (int i = 0; i < xfersizes.length; i+=2)
      max = Math.max(max, xfersizes[i]);

    return(int) max;
  }


  public void setShutdown(boolean bool)
  {
    shutting_down = bool;
  }
  public boolean getShutdown()
  {
    return shutting_down;
  }


  public int getOperation()
  {
    if (operation < 0)
      common.failure("Negative operation");
    return operation;
  }

  public void setOperation(int op)
  {
    if (op < 0)
      common.failure("Negative operation");
    operation = op;
  }

  public boolean compareSizes(double[] sizes)
  {
    if (filesizes == null || sizes == null)
      return false;

    if (filesizes.length != sizes.length)
      return false;

    for (int i = 0; i < filesizes.length; i++)
    {
      if (filesizes[i] != sizes[i])
        return false;
    }

    return true;
  }


  /**
   * A copy or move operation allows the use of the 'target=' parameter.
   * This allows the target of the operation to be a different FSD than the main
   * FSD used in this workload.
   */
  private void setTargetFsd(FwgEntry fwg_name, FwdEntry fwd)
  {
    if (fwd.target_fsd != null)
    {
      for (int i = 0; i < FsdEntry.getFsdList().size(); i++)
      {
        FsdEntry fsd = (FsdEntry) FsdEntry.getFsdList().elementAt(i);
        if (!fwd.target_fsd.equals(fsd.name))
          continue;

        fwg_name.target_anchor = fsd.anchor;

        /* The structure of these FSDs must be identicial: */
        if (fsd.depth != depth ||
            fsd.width != width ||
            fsd.files != files ||
            fsd.filesizes.length != filesizes.length)
          common.failure("FSD structure must be identical for anchor=" + anchor.getAnchorName() +
                         " and 'target=" + fwd.target_fsd + "'");

        for (int j = 0; j < fsd.filesizes.length; j++)
        {
          if (fsd.filesizes[j] != filesizes[j])
            common.failure("FSD structure must be identical for anchor=" + anchor.getAnchorName() +
                           " and 'target=" + fwd.target_fsd + "'");
        }

        return;
      }

      common.failure("'target=" + fwd.target_fsd + "'. FSD not found");
    }
  }


  public String toString()
  {
    printf pf = new printf("fwg=%-8s op=%-6s sel=%-4s sq=%-4s sk=%3d th=%2d an=%-8s wd=%4d dp=%4d fl=%4d to=%d");
    pf.add(fwg_name);
    pf.add(Operations.getOperationText(operation));
    pf.add(select_random);
    pf.add(sequential_io);
    pf.add(skew);
    pf.add(threads);
    pf.add(anchor.getAnchorName());
    pf.add(width);
    pf.add(depth);
    pf.add(files);
    pf.add(total_size);

    return Format.f("fwg:%4d ", seqno) + pf.text;
  }


  public int compareTo(Object obj)
  {
    return fwg_name.compareTo( ((FwgEntry) obj).fwg_name);
  }
}
