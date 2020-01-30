package Utils;

/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */

/*  
 * Author: Henk Vandenbergh. 
 */

import java.util.*;

/**
 * All native functions plus a conversion method
 */
public class NfsStat {
  private final static String c = "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";

  /**
   * Get a kstat_ctl_t pointer to Kstat
   */
  private static native long kstat_open();

  /**
   * Close kstat kstat_ctl_t pointer
   */
  private static native long kstat_close(long kstat_ctl_t);

  /**
   * Using named Kstat data, return a String with label number label number .....
   *
   * Can return with String: "JNI failure: ...."
   */
  private static native String kstat_lookup_stuff(long kstat_ctl_t, String module, String name);

  /**
   * Parse data received from Jni into a new NamedData instance. Input data starts
   * with all labels and ends with all counters, with an asterix in between.
   */
  public static void obsolete_parseNfsData(NamedData nd, String data) {
    if (data.indexOf("*") == -1)
      common.failure("Invalid data syntax: " + data);

    /* Get all the field names: */
    String field_string = data.substring(0, data.indexOf("*")).trim();
    String[] fields;
    if (field_string.indexOf("$") == -1)
      fields = field_string.split(" +");
    else
      fields = field_string.split("$+");

    /* Get all the counters: */
    String counter_string = data.substring(data.indexOf("*") + 1).trim();
    String[] counters = counter_string.split(" +");

    if (fields.length != counters.length) {
      common.ptod("counters.length: " + counters.length);
      common.ptod("field_string: " + field_string);
      common.ptod("counter_string: " + counter_string);
      common.ptod(field_string);
      common.ptod(counter_string);
      common.failure("Unequal token count. Receiving more labels than expected");
    }

    /* If any new fields came in add them: */
    nd.validateLookupTable(fields);

    /* Store the counters in the proper place decided by the label: */
    for (int i = 0; i < fields.length; i++) {
      Lookup look = nd.getLookupForField(fields[i]);

      /* If we can't find this field we're in trouble: */
      if (look == null)
        common.ptod("Lookup missing for field: " + fields[i]);

      nd.setCounter(look, Long.parseLong(counters[i]));
      // common.ptod("counters[i]: " + counters[i] + " " + fields[i]);
    }
  }

  public static void main(String[] args) {
    /*
     * Swt.common.load_shared_library();
     * 
     * long kstat_ctl_t = kstat_open();
     * 
     * String data = kstat_lookup_stuff(kstat_ctl_t, "nfs", "rfsreqcnt_v3");
     * 
     * common.ptod("data: " + data);
     * 
     * StringTokenizer st = new StringTokenizer(data); while (st.hasMoreTokens()) {
     * common.ptod(st.nextToken()); }
     * 
     * kstat_close(kstat_ctl_t);
     */
  }
}
