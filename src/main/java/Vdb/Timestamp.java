package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import Utils.*;


/**
 * This class will handle the storing and retrieving of Data Validation
 * timestamps.
 * From 'validate=time'.
 *
 * This is the first version, still using 8 bytes per block.
 * Next planned version: 4 bytes per block which leaves 32-4=28 bits for a
 * millisecond timestamp, or about 75 hours of run time.
 * We can improve on that by storing less detail:
 * - every 2ms:  150 hours
 * - every 10ms: 750 hours or 31 days.
 *
 * We'll still have a limit of Integer.MAX_VALUE blocks though unless we tackle
 * that also.
 *
 * 03/16/2017: now also storing the xfersize used for the last write for a key
 * block. That info was very powerful with our last corruption.
 * Now also storing here the timestamp USED wjen creating the data pattern,
 * instead of a separate, little later, timestamp.
 */
public class Timestamp
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private long[] timestamps  = null;
  private int [] write_xfers = null;
  private int    entries = 0;

  private static int MAX_SIZE = Integer.MAX_VALUE;

  /**
   * If the 'validate=time' option is used, save the last successful TS.
   * The flags below are stored in byte0 over the timestamp.
   *
   * Note that they don't really need to be bit flags, any 4bit integer will do.
   */
  private static long TIME_MASK      = 0x0000ffffffffffffl;
  private static long OPERATION_MASK = 0xff00000000000000l;
  private static long KEY_MASK       = 0x00ff000000000000l;
  public  static long READ_ONLY      = 0x1000000000000000l;
  public  static long PRE_READ       = 0x2000000000000000l;
  public  static long READ_IMMED     = 0x3000000000000000l;
  public  static long WRITE          = 0x4000000000000000l;
  public  static long PENDING_READ   = 0x5000000000000000l;
  public  static long PENDING_REREAD = 0x6000000000000000l;



  public Timestamp(long blocks)
  {
    common.ptod("Data Validation. Allocating timestamp map "+
                "requiring 8 * %,d = %.3fMB of java heap space.",
                blocks, blocks * 8. / 1024. / 1024.);

    if (blocks > MAX_SIZE)
      common.failure("Timestamp map only supports %,d entries", MAX_SIZE);

    entries = (int) blocks;
    timestamps  = new long[ entries ];
    if (Validate.isXferHistory())
      write_xfers = new int [ entries ];
  }


  public static void main(String[] args)
  {
    long blocks = common.parseSize(args[0]);
    Timestamp ts = new Timestamp(blocks);
  }

  /**
   *  Store timestamp of last successful i/o, including flags
   *
   *  Current key value is also stored here. In theory the key value here may be
   *  running just a little ahead of the Data Validation map (DV_map), because
   *  the current key value is stored back in the map AFTER error reporting.
   *
   *  If you need the one byte taken up here, get the info from DV_map instead,
   *  therewith accepting the very small window.
   */
  public void storeTime(long block, long type, long tod, long key)
  {
    if (block > entries)
      common.failure("Timestamp: requesting block %,d which is larger than the "+
                     "current size of %,d", block, entries);

    timestamps[ (int) block ] = tod | type | key << 48;
  }

  public void storeWriteXfer(long block, int xfer)
  {
    if (block > entries)
      common.failure("Timestamp: requesting block %,d which is larger than the "+
                     "current size of %,d", block, entries);

    write_xfers[ (int) block ] = xfer;
  }


  /**
   * Get the last stored timestamp value, EXCLUDING the flags.
   */
  public long getLastTime(long block)
  {
    if (block > entries)
      common.failure("Timestamp: requesting block %,d which is larger than the "+
                     "current size of %,d", block, entries);

    long value = timestamps [ (int) block ];

    /* Remove the (possible) flags: */
    return(value & TIME_MASK);
  }

  public int getLastXfersize(long block)
  {
    if (block > entries)
      common.failure("Timestamp: requesting block %,d which is larger than the "+
                     "current size of %,d", block, entries);

    return write_xfers [ (int) block ];
  }

  public String getLastOperation(long block)
  {
    if (block > entries)
      common.failure("Timestamp: requesting block %,d which is larger than the "+
                     "current size of %,d", block, entries);

    long value = timestamps [ (int) block ];
    long type  = value & OPERATION_MASK;
    if (type == READ_ONLY )   return "read";
    if (type == PRE_READ  )   return "pre_read";
    if (type == READ_IMMED)   return "read_after_write";
    if (type == WRITE     )   return "written";
    if (type == PENDING_READ) return "journal pending read";
    return "unkown_operation";
  }

  public int getLastKey(long block)
  {
    if (block > entries)
      common.failure("Timestamp: requesting block %,d which is larger than the "+
                     "current size of %,d", block, entries);

    long value = timestamps [ (int) block ];
    long key   = value & KEY_MASK;
    key = key >> 48;
    return(int) key;
  }
}

