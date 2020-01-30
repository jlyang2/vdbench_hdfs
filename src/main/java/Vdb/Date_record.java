package Vdb;

/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */

/*  
 * Author: Henk Vandenbergh. 
 */

import java.util.*;
import java.text.*;
import Utils.Bin;

/**
 * Date record for binary file. This sets the date and time of day for when the
 * data was collected.
 *
 * Timestamps are in truncated seconds. This eliminates the possiblity that
 * intervals get thrown in a wrong seckond because of the timestamp being a few
 * milliseconds off.
 */
class Date_record extends Bin_record {
  private final static String c = "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";

  Date start = null;
  Date end = null;

  static long timezone_offset = 0; /* These two only used for emport() */
  String timezone = "n/a";

  public static long dflt_timezone_offset = new Date().getTimezoneOffset() * 60 * 1000;
  private static String dflt_timezone = TimeZone.getDefault().getID();

  private static GmtFormat df = new GmtFormat("MMddyyyy-HH:mm:ss.SSS");

  /* Added version 1 that now includes the GMT offset and the timezone */
  final static byte record_version = 1;

  Date_record() {
    super(Bin.DATE_RECORD, record_version);
  }

  Date_record(Date start, Date end) {
    super(Bin.DATE_RECORD, record_version);
    this.start = start;
    this.end = end;
    this.timezone_offset = start.getTimezoneOffset();
  }

  /**
   * Convert this instance to the binary data format.
   */
  public void export(Bin bin) {
    bin.put_long(start.getTime());
    bin.put_long(end.getTime());
    bin.put_long(dflt_timezone_offset);
    bin.put_string(dflt_timezone);

    bin.write_record(Bin.DATE_RECORD, record_version);
  }

  /**
   * Convert exported instance back to java.
   *
   * When we get a timestamp equal to zero we need to adjust that timestamp to
   * conform to our timezone. Otherwise the timestamp would be printed adjusted
   * for the local timezone, e.g. new Date(0) would be printed as
   * 12311969-17:00:01.000
   */
  public void emport(Bin bin) {
    if (bin.record_type != Bin.DATE_RECORD)
      common.failure(
          "Date_record.emport(): Expecting record type " + Bin.DATE_RECORD + ", but reading " + bin.record_type + ".");
    long dt = bin.get_long();

    start = new Date(dt);
    end = new Date(bin.get_long());

    if (record_version > 0) {
      timezone_offset = bin.get_long();
      timezone = bin.get_string();
    }
  }

  public String toString() {
    return "d " + df.format(start) + " " + df.format(end);
  }
}
