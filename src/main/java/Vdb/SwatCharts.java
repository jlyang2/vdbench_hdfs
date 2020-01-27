package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.util.Vector;
import java.io.*;
import Utils.CommandOutput;
import Utils.OS_cmd;
import Utils.Fget;
import Utils.Fput;
import Utils.Format;

/**
 * Code to handle the automatic creation of Swat charts.
 */
public class SwatCharts
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private String where_is_swat;
  private String which_charts;
  private String chart_directory;
  private String index;

  private static SwatCharts charts = null;
  private static boolean debug = false;

  public SwatCharts(String dir, String file)
  {
    where_is_swat = dir;
    which_charts  = file;
    charts        = this;

    if (!new File(which_charts).exists())
      common.failure("SwatCharts: file does not exist: " + which_charts);

    if (!debug)
    {
      /* Create new directory, or if already there, remove all existing files: */
      chart_directory = reporting.rep_mkdir(Vdbmain.output_dir + File.separator + "charts");
      index           = chart_directory + File.separator + "index.html";
      Report.getSummaryReport().println(Format.f("%-32s", "Link to Swat charts") +
                                        " <A HREF=\"" + "charts/index.html" + "\">charts</A>");
    }
  }

  public static SwatCharts getCharts()
  {
    return charts;
  }

  public static void createCharts()
  {
    if (charts == null)
      return;

    Tnfe_data.close();

    OS_cmd ocmd = new OS_cmd();
    ocmd.addQuot(charts.where_is_swat + File.separator + "swat");
    ocmd.addText("reporter");
    ocmd.addQuot("dir=" + Vdbmain.output_dir);
    ocmd.addQuot("target=" + Vdbmain.output_dir + File.separator + "charts");

    String[] lines = Fget.readFileToArray(charts.which_charts);
    for (int i = 0; i < lines.length; i++)
    {
      if (!lines[i].startsWith("*"))
        ocmd.addText(lines[i]);
    }

    //ocmd.addText("-d56");

    //common.ptod("ocmd: " + ocmd.getCmd());


    ocmd.setOutputMethod(new CommandOutput()
                         {
                           public boolean newLine(String line, String type, boolean more)
                           {
                             if (debug)
                               common.ptod(type + ": " + line);
                             else
                               common.plog(type + ": " + line);
                             return true;
                           }
                         });

    common.ptod("Starting Swat chart creation.");
    boolean rc = ocmd.execute(false);
    if (rc)
    {
      common.ptod("Completed Swat charts in directory " +
                  Vdbmain.output_dir + File.separator + "charts");
      charts.createIndex();
    }
    else
      common.ptod("Error while creating Swat charts. Please see messages in logfile.html");

  }

  /**
   * Create file output/charts/index.html with all the file names.
   */
  private void createIndex()
  {
    String[] files = new File(chart_directory).list();
    Fput fp = new Fput(index);
    fp.println("List of file names containing Swat charts created by Swat:\n\n");

    for (int i = 0; i < files.length; i++)
    {
      String file = files[i];
      if (file.endsWith(".jpg"))
        fp.println("<A HREF=\"" + file + "\">" + file + "</A>");
    }
    fp.close();
  }

  public static void main(String[] args)
  {
    debug = true;
    SwatCharts ch = new SwatCharts("h:\\swat", "swatcharts.txt");
    ch.createCharts();
  }

}
