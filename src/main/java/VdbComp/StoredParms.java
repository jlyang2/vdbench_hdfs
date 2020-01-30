package VdbComp;

/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.awt.Color;
import java.io.*;
import java.util.*;

import Utils.Fget;
import Utils.Fput;
import Utils.common;

/**
 * This class handles parameters saved across sessions.
 *
 */
public class StoredParms {
  private final static String c = "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";

  static int last_width = 800;
  static int last_height = 400;
  static int last_x = 20;
  static int last_y = 20;

  public static void loadParms() {
    Delta[] deltas = Delta.getDeltas();
    double[] limits = new double[deltas.length];
    ;

    String ini = Utils.ClassPath.classPath("wlcomp.ini");
    if (!Fget.file_exists(ini))
      return;

    Fget fg = new Fget(ini);
    int idx = 0;
    String line = null;
    while ((line = fg.get()) != null) {
      String[] split = line.trim().split(" +");

      if (split[0].equals("delta")) {
        limits[idx] = Double.parseDouble(split[1]);
        if (split.length > 2) {
          deltas[idx].color = new Color(Integer.parseInt(split[2]), Integer.parseInt(split[3]),
              Integer.parseInt(split[4]));
          deltas[idx].setBackground(deltas[idx].color);
        }
        idx++;
      }

      else if (split[0].equals("width"))
        last_width = (int) Double.parseDouble(split[1]);

      else if (split[0].equals("height"))
        last_height = (int) Double.parseDouble(split[1]);

      else if (split[0].equals("x"))
        last_x = (int) Double.parseDouble(split[1]);

      else if (split[0].equals("y"))
        last_y = (int) Double.parseDouble(split[1]);
    }

    fg.close();

    /* If there were any deltas in the input, copy them: */
    if (limits != null)
      Delta.setDeltas(limits);
  }

  public static void storeParms() {
    String ini = Utils.ClassPath.classPath("wlcomp.ini");

    Fput fp = new Fput(ini);

    fp.println("width " + WlComp.frame.getSize().getWidth());
    fp.println("height " + WlComp.frame.getSize().getHeight());
    fp.println("x " + WlComp.frame.getLocation().getX());
    fp.println("y " + WlComp.frame.getLocation().getY());

    Delta[] deltas = Delta.getDeltas();
    for (int i = 0; i < deltas.length; i++)
      fp.println("delta " + deltas[i].limit + " " + deltas[i].color.getRed() + " " + deltas[i].color.getGreen() + " "
          + deltas[i].color.getBlue());

    fp.close();
  }
}
