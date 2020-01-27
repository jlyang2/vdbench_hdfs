package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.*;
import Utils.Format;



/**
 * Handle the requested filesystem workload.
 *
 */
abstract class FwgThread extends Thread
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  protected FwgEntry fwg;

  protected Task_num tn;

  private   int seqno;

  protected ActiveFile afe = null;

  public    long   consecutive_blocks = 0;
  public    long   last_ok_request    = System.currentTimeMillis();
  private   int     last_block          = 0;

  private   long    native_read_buffer  = 0;
  private   long    native_write_buffer = 0;

  public    long    permit_last = 0;
  public    long    permit_time = 0;
  public    long    permit_tod  = 0;


  protected boolean format;
  protected boolean format_restart;
  protected int     operation;
  private   boolean expect_rmdirs;

  private int buffer_size = 0;

  /* Counters when running on slave: */
  public FwdStats per_thread_stats = new FwdStats();
  public FwdStats old_stats        = new FwdStats();

  protected static long YEAR = 365 * 24 * 60 * 60;

  abstract protected boolean doOperation() throws InterruptedException;

  protected int OUTPUT_FILE_MUST_EXIST    = 99;
  protected int OUTPUT_FILE_MAY_NOT_EXIST = 98;
  protected int OUTPUT_FILE_EITHER        = 97;
  protected int OUTPUT_FILE_NOT_COMPLETE  = 96;  /* Either non-existent or not correct size */

  private   static Object shutdown_lock = new Object();

  private   static boolean fast_block_kill = common.get_debug(common.FAST_BLOCK_KILL);
  private   static boolean report_permits  = common.get_debug(common.REPORT_FWG_PERMITS);
  private   static int BLOCK_KILL = (fast_block_kill) ? 100 : 10000;
  private   static int thread_number = 0;



  /**
   * Create a new work thread.
   */
  public FwgThread(Task_num tn, FwgEntry fwg)
  {
    seqno = thread_number++;
    if (tn != null)
      this.setName(tn.task_name + " rd=" + SlaveWorker.work.work_rd_name);
    else
      this.setName("notask " + " rd=" + SlaveWorker.work.work_rd_name);
    this.tn             = tn;
    this.fwg            = fwg;
    this.format         = SlaveWorker.work.format_run;
    this.format_restart = format && SlaveWorker.work.format_flags.format_restart;
    this.operation      = fwg.getOperation();
    this.expect_rmdirs  = SlaveWorker.canWeExpectDirectoryDeletes();

    /* Each thread gets its own buffers, but only when really needed: */
    if (this instanceof OpCreate  ||
        this instanceof OpRead    ||
        this instanceof OpWrite   ||
        this instanceof OpCopy    ||
        this instanceof OpMove )
    {
      buffer_size = fwg.getMaxXfersize();

      /* Dedup needs a minimum of dedupunit plus extra for straddling across units: */
      if (fwg.dedup != null)
      {
        buffer_size = Math.max(buffer_size, fwg.dedup.getDedupUnit());
        buffer_size += fwg.dedup.getDedupUnit();
      }

      /* Why allocate a read buffer when not reading? Only for DV (pre/post)read */
      /* (We don't really care about resource usage for DV anyway)               */
      if (Validate.isRealValidate()      ||
          this instanceof OpRead         ||
          this instanceof OpReadWrite    ||
          this instanceof OpWrite        ||
          this instanceof OpCopy         ||
          this instanceof OpMove )
      {
        native_read_buffer = Native.allocBuffer(buffer_size);
      }

      /* Unless this is clearly read-only, create a write buffer: */
      boolean need_buffer = false;
      if (tn == null)
        need_buffer = true;
      else if (this instanceof OpReadWrite)
        need_buffer = true;
      else if (!(this instanceof OpRead))
        need_buffer = true;

      //if (tn == null || !(this instanceof OpRead))
      if (need_buffer)
      {
        /* Since we started writing from the pattern buffer we no */
        /* longer need a separate write buffer:                   */
        // This change was pulled out until further notice because
        // of the introduction of too much unwanted dedup
        //if (Validate.isRealValidate() || Validate.isValidateForDedup())
        {
          native_write_buffer = Native.allocBuffer(buffer_size);
          Patterns.storeStartingFsdPattern(native_write_buffer, buffer_size);
        }
      }
    }

    /* Fake threads for 'format' should not be marked pending */
    if (tn != null)
      tn.task_set_start_pending();

  }

  /**
   * Finalize, to make sure buffers are cleaned up.
   */
  public void finalize() throws Throwable
  {
    if (native_read_buffer != 0)
      Native.freeBuffer(buffer_size, native_read_buffer);
    if (native_write_buffer != 0)
      Native.freeBuffer(buffer_size, native_write_buffer);
    super.finalize();
  }



  public void run()
  {
    Thread.currentThread().setPriority(Thread.MIN_PRIORITY);

    /* Get the proper queue element for the FwgEntry: */
    FwgQueue queue = FwgWaiter.getMyQueue(fwg);

    try
    {
      boolean controlled_format = common.get_debug(common.USE_FORMAT_RATE);

      tn.task_set_start_complete();
      tn.waitForMasterGo();

      /* A format run may have to delete some old files: */
      if (fwg.anchor.isDeletePending())
        fwg.anchor.cleanupOldFiles(fwg);

      /* Large workload loop: */
      while (!SlaveJvm.isWorkloadDone())
      {
        /* Threads can shut down because they have no more work: */
        // I am starting to question this shutdown:
        // Yes, there may no longer be no (new files), but there still
        // may be some threads that need to finish writing are reading to EOF!
        // And with select_once those things need to finish.
        // (it appears that now have have 'touch once'.)
        // And that is what was originally planned.
        // So, fileselect=once stays as it is, but it would be nice to fix that.
        if (fwg.getShutdown())
          break;

        /* Wait for my chance to do something: */
        if (!format && !Validate.isJournalRecoveryActive())
        {
          try
          {
            /* This code here allows a user to report how busy a thread is.  */
            /* This gets very helpful when one or more threads are throttled */
            /* because of a 'controlled' workload.                           */
            /* Info will be reported at the end of each detail line.         */
            if (!report_permits)
              queue.getPermit();
            else
            {
              long start = System.nanoTime();
              queue.getPermit();
              permit_time += (System.nanoTime() - start);
            }
            //common.ptod("queue: " + queue.fwg.fsd_name + " " + queue.fwg.getOperation() + " " +  queue.releases);
          }
          catch (InterruptedException e)
          {
            break;
          }
        }

        if (!doOperation())
        {
          /* The 'suspend' below here prevented a thread like OPWrite from */
          /* finishing the file that he was writing. */
          /* With that, FwgWaiter.run ended up being stuck in a loop when */
          /* there were no other Fwg's running. */
          //FwgWaiter.getMyQueue(fwg).suspendFwg();
          break;
        }

        /* Reset the counter that causes us to abort after 10,000 blocks: */
        consecutive_blocks = 0;
        last_ok_request    = System.currentTimeMillis();
      }

      /* Make sure any open file gets closed: */
      ActiveFile.conditionalCloseFile(afe);

      tn.task_set_terminating(0);
    }

    catch (Throwable t)
    {
      common.abnormal_term(t);
    }

    /* If every task is gone, shut down: */
    synchronized (shutdown_lock)
    {
      /* Earlier use here of countTasks("FwgThread") did not include threads */
      /* that had not even reached 'RUNNING' and we shut down too early      */
      if (Task_num.checkAllInTermination())
        SlaveJvm.setWorkloadDone(true);

    }

    //common.ptod("Ending FwgThread for fwg=" + fwg.fsd_name);
  }


  /**
   * Look for a file to create. Loop until you find one.
   */
  protected FileEntry findNonExistingFile()
  {
    while (!SlaveJvm.isWorkloadDone())
    {
      FileEntry fe = fwg.anchor.getFile(fwg);
      if (fe == null)
        return null;

      if (!fe.setFileBusyExc())
      {
        block(Blocked.FILE_BUSY);
        continue;
      }

      if (fe.isBadFile())
      {
        block(Blocked.BAD_FILE_SKIPPED);
        fe.setUnBusy();
        continue;
      }

      /* During format restart the file may exist, and */
      /* will be used if it is not full yet:           */
      if (format_restart && fe.exists())
      {
        if (!fe.isFull())
          return fe;
      }

      /* If the file is already there during a format, continue: */
      if (format && fe.exists())
      {
        // common.ptod("fe: " + fe.getName() + " exists: " + fe.exists());
        fe.setUnBusy();

        /* If this were just a simple format we could now end it for this thread.  */
        /* However, since it may be a 'fortotal' or 'restart' we need to continue  */
        /* looking for files that do NOT exist yet.                                */
        /* It generates a lot of blocks that may be confusing, so I decided to     */
        /* remove the block and reporting here. It also avoids the sleep in block. */
        block(Blocked.FILE_MAY_NOT_EXIST);

        /* It can be that the file count needed to reach zero here needs */
        /* to wait for an other thread to complete it's file creation.   */
        /* We'll therefore sleep a bit to prevent wasting loops:         */
        //if (fwg.anchor.anyMoreFilesToCreate() == 0)
        if (fwg.anchor.allFilesFull() )
          return null;

        common.sleep_some_usecs(200);
        continue;
      }

      /* Block for loads of reasons: */
      if (fe.exists())
      {
        fe.setUnBusy();

        String[] txt =
        {
          "Anchor: " + fwg.anchor.getAnchorName(),
          "Vdbench is trying to create a new file, but all files already exist, ",
          "and no threads are currently active deleting files"
        };
        if (!canWeGetMoreFiles(txt))
        {
          return null;
        }

        block(Blocked.FILE_MAY_NOT_EXIST);
        continue;
      }

      /* We need to make sure the parent is locked to make sure that he */
      /* stays around: */
      // Wrong: the file is locked so the parent never can go away!!!!
      // wrong again: the file does not exist yet!
      if (expect_rmdirs && !format && !fe.setParentBusy(true))
      {
        fe.cleanup();
        block(Blocked.PARENT_DIR_BUSY);
        continue;
      }

      if (!fe.getParent().exist())
      {
        fe.cleanup();
        //common.ptod("fe: " + fe.getName());
        block(Blocked.MISSING_PARENT);
        continue;
      }

      return fe;
    }

    return null;
  }

  protected boolean doSequentialWrite(boolean stay_with_same_file)
  {
    /* Get the next transfer size: */
    afe.xfersize = fwg.getXferSize();

    /* If we just reached EOF, get an other file: */
    if (!afe.setNextSequentialWrite() || afe.done_enough)
    {
      if (format)
        afe.getFileEntry().setFormatComplete(true);
      afe = afe.closeFile();

      if (stay_with_same_file)
      {
        afe = null;
        return false;
      }

      /* For restart we look for files that are empty or half full: */
      FileEntry fe;
      if (format_restart)
        fe = findFileToWrite(OUTPUT_FILE_NOT_COMPLETE);
      else
        fe = findFileToWrite(OUTPUT_FILE_EITHER);
      if (fe == null)
        return false;

      afe = openForWrite(fe);

      /* Get the first transfer size: */
      afe.xfersize = fwg.getXferSize();

      /* And get the lba for this new file.                                       */
      /* It can (and has been proven) that this call can determine that the whole */
      /* (likely small) file has been identified to be bad because of a DV error. */
      /* This then results in xfersize=0                                          */
      /* This STILL needs to be fixed though!!!                                   */
      /* (The 'proven' one was when stopafter was accidentally recognized         */
      /*  for mixed random+sequential workloads)                                  */
      boolean rc = afe.setNextSequentialWrite();
    }

    afe.writeBlock();

    /* Determine if we've done enough: */
    afe.checkEnough();

    return true;
  }

  /**
   * Note: open/close gives 8 getattrs and 7 access
   */
  protected ActiveFile obsolete_openFile(FileEntry fe)
  {
    if (fwg.getOperation() == Operations.WRITE || fwg.readpct >= 0)
      return openForWrite(fe);
    else
      return openForRead(fe);
  }


  protected ActiveFile openForRead(FileEntry fe)
  {
    ActiveFile afe = new ActiveFile(fe, fwg, native_read_buffer, native_write_buffer);
    long start = Native.get_simple_tod();
    afe.openFile(true);
    fwg.blocked.count(Blocked.READ_OPENS);
    FwdStats.count(Operations.OPEN, start);

    return afe;
  }

  /**
   * Open a file for output.
   * If this is a format restart, set the starting lba to the current file size
   */
  protected ActiveFile openForWrite(FileEntry fe)
  {
    /* Delete if we asked for it: */
    //common.ptod("fwg.sequential_io: " + fwg.sequential_io + " " +
    //            fwg.del_b4_write + " " + fe.exists());
    if (fwg.sequential_io && fwg.del_b4_write && fe.exists())
    {
      //common.ptod("fwg.stopafter == 0:  " + fwg.stopafter);
      //common.ptod("fe.getCurrentSize(): " + fe.getCurrentSize());
      //common.ptod("fe.getReqSize():     " + fe.getReqSize());
      if (fwg.stopafter == 0 || fe.getCurrentSize() == fe.getReqSize())
        fe.deleteFile(fwg);
    }

    ActiveFile afe = new ActiveFile(fe, fwg, native_read_buffer, native_write_buffer);
    long start = Native.get_simple_tod();
    afe.openFile(false);
    fwg.blocked.count(Blocked.WRITE_OPENS);
    FwdStats.count(Operations.OPEN, start);

    /* Count the creates done for writes (operation=create counts himself): */
    //if (fwg.sequential_io && fwg.del_b4_write && !fe.exists())
    if (fwg.getOperation() == Operations.WRITE && !fe.exists())
    {
      //common.where();
      //fwg.blocked.count(Blocked.FILE_CREATES);
      FwdStats.count(Operations.CREATE, start);
    }

    /* When restarting, set the starting point of the file to xfersize multiple. */
    if (format_restart)
    {
      long xfersize = fwg.getXferSize();
      afe.next_lba = (fe.getCurrentSize() / xfersize) * xfersize;
    }

    return afe;
  }



  /**
   * Find a file that is not busy and open it for processing
   */
  public FileEntry findFileToWrite(int find_option)
  {
    FileEntry fe;

    //common.ptod("fwg: " + fwg.target_anchor + " " + fwg.getOperation() + " " + format);
    while (!SlaveJvm.isWorkloadDone())
    {
      if (!format &&
          (fwg.getOperation() == Operations.COPY ||
           fwg.getOperation() == Operations.MOVE))
        fe = fwg.target_anchor.getFile(fwg);
      else
        fe = fwg.anchor.getFile(fwg);
      if (fe == null)
        return null;

      /* If file already busy, try an other: */
      boolean rc = (fwg.del_b4_write) ? fe.setFileBusyExc() : fe.setFileBusy();
      if (!rc)
      {
        block(Blocked.FILE_BUSY);
        continue;
      }

      if (fe.isBadFile())
      {
        block(Blocked.BAD_FILE_SKIPPED);
        fe.setUnBusy();
        continue;
      }

      /* The file must already exist: */
      if (find_option == OUTPUT_FILE_MUST_EXIST && !fe.exists())
      {
        fe.setUnBusy();

        String[] txt =
        {
          "Anchor: " + fwg.anchor.getAnchorName(),
          "Vdbench is trying to write to a file, but no files are available, and no",
          "threads are currently active creating new files: " +
          fwg.anchor.getAnchorName()
        };

        if (!canWeGetMoreFiles(txt))
          return null;

        block(Blocked.FILE_MUST_EXIST);
        continue;
      }


      /* The file may not already exist: */
      if (find_option == OUTPUT_FILE_MAY_NOT_EXIST && fe.exists())
      {
        fe.setUnBusy();

        String[] txt =
        {
          "Anchor: " + fwg.anchor.getAnchorName(),
          "Vdbench is trying to write to a file, but it needs a file that does"+
          "not exist yet, and no threads are currently active deleting files."
        };
        if (!canWeExpectFileDeletes(txt))
          return null;

        block(Blocked.FILE_MAY_NOT_EXIST);
        continue;
      }

      /* The file may not be complete: */
      //common.ptod("attempt: " + fe.getName() +
      //            " exists: " + fe.exists() + " fmcompl: " + fe.isFormatComplete() +
      //            " isfull: " + fe.isFull());
      if (find_option == OUTPUT_FILE_NOT_COMPLETE && fe.exists() && fe.isFull())
      {
        fe.setUnBusy();

        block(Blocked.FILE_IS_FULL);

        /* Prevent spinning when other threads are finishing up: */
        if (format)
          common.sleep_some(200);

        if (fe.getAnchor().allFilesFull())
          return null;

        continue;
      }

      /* For random i/o the file must be full: */
      //if (!fwg.sequential_io && !fe.isFull())
      //{
      //  fe.setBusy(false);
      //  block(Blocked.FILE_NOT_FULL);
      //  continue;
      //}

      /* find_option == OUTPUT_FILE_EITHER */

      /* Format requires us to stop after the last file: */
      if (format && fe.isFormatComplete())
      {
        fe.setUnBusy();
        return null;
      }

      /* Are we formatting a file that is already full? */
      if (format && fe.isFull())
      {
        fe.setFormatComplete(true);
        fe.setUnBusy();
        continue;
      }

      /* Of course, the parent must already be there: */
      if (!fe.getParent().exist())
      {
        fe.setUnBusy();
        block(Blocked.MISSING_PARENT);
        continue;
      }

      return fe;
    }

    return null;
  }


  /**
   * Get a FileEntry for read operations.
   *
   * Note: During normal operations a file is always full. However after
   * journal recovery a file possibly is NOT full because we terminated
   * while filling it!
   * This needs to be checked to make sure we don't cause problems.
   */
  protected FileEntry findFileToRead()
  {
    while (!SlaveJvm.isWorkloadDone())
    {
      FileEntry fe = fwg.anchor.getFile(fwg);

      /* 'null' means Journal recovery just completed: */
      if (fe == null)
        return null;

      /* If file is busy, try an other one: */
      if (!fe.setFileBusy())
      {
        block(Blocked.FILE_BUSY);
        continue;
      }

      if (fe.isBadFile())
      {
        block(Blocked.BAD_FILE_SKIPPED);
        fe.setUnBusy();
        continue;
      }

      /* The file must already exist: */
      if (!fe.exists())
      {
        fe.setUnBusy();
        block(Blocked.FILE_MUST_EXIST);

        if ((operation == Operations.MOVE || operation == Operations.COPY) &&
            fwg.anchor.getExistingFileCount() != 0)
          continue;

        String[] txt =
        {
          "Anchor: " + fwg.anchor.getAnchorName(),
          "Vdbench is trying to read from a file, but no files are available, and no",
          "threads are currently active creating new files"
        };

        if (!canWeGetMoreFiles(txt))
          return null;

        continue;
      }

      /* Journal recovery only reads the files that have any block written: */
      // activated again to assist in journal recovery // Code removed. Is now handled by FileEntry.setBlockBad()
      if (Validate.isJournalRecoveryActive())
      {
        /* Mark file bad if needed: */
        if (fwg.anchor.getDVMap().anyBadBlocks(fe.getFileStartLba(), fe.getCurrentSize()))
        {
          //fe.setBadFile();
          fe.setUnBusy();
          continue;
        }

        /* File only will be read if the file has ever been written: */
        if (!fwg.anchor.getDVMap().anyValidBlocks(fe.getFileStartLba(), fe.getCurrentSize()))
        {
          fe.setUnBusy();
          continue;
        }
      }

      /* The file may not be empty: */
      if (fe.getCurrentSize() == 0)
      {
        fe.setUnBusy();
        block(Blocked.FILE_NOT_FULL);
        continue;
      }

      //common.ptod("findFileToRead: " + fe.getName() + " " + fe.getCurrentSize());
      return fe;
    }

    return null;
  }

  /**
   * See if we can continue.
   *
   * It can happen that there just are no more files to create.
   * In that case, just shutdown
   */
  protected boolean canWeGetMoreFiles(String[] msg)
  {
    if (fwg.anchor.anyMoreFilesToCreate() == 0)
    {
      /* No more files to create. Can we expect any deletes? */
      if (SlaveWorker.canWeExpectFileDeletes())
        return true;

      synchronized (fwg)
      {
        if (!SlaveWorker.work.format_run && !fwg.getShutdown())
        {
          fwg.setShutdown(true);
          sendMessage(msg, "FwgThread.canWeGetMoreFiles(): Shutting down threads "+
                      "for operation=" +
                      Operations.getOperationText(operation));

        }

        /* We have all files we need, so let format know about that: */
        if (SlaveWorker.work.format_run)
        {
          return false;
        }
      }

      return false;
    }

    /* Maybe we have enough potential files, but is anyone creating new ones? */
    if (fwg.anchor.getExistingFileCount() == 0)
    {
      if (!SlaveWorker.canWeExpectFileCreates())
      {
        synchronized (fwg)
        {
          if (!SlaveWorker.work.format_run && !fwg.getShutdown())
          {
            fwg.setShutdown(true);
            sendMessage(msg, "FwgThread.canWeGetMoreFiles(): Shutting down "+
                        "threads for operation=" +
                        Operations.getOperationText(operation));
          }
        }
        return false;
      }
    }


    return true;
  }


  public boolean canWeExpectFileDeletes(String[] msg)
  {
    Work work = SlaveWorker.work;

    /* Go through all FWDs looking for file deletes: */
    for (int i = 0; i < work.fwgs_for_slave.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) work.fwgs_for_slave.elementAt(i);
      if (fwg.getOperation() == Operations.DELETE)
        return true;
      if (fwg.getOperation() == Operations.MOVE)
        return true;
    }

    synchronized (fwg)
    {
      if (!fwg.getShutdown())
      {
        fwg.setShutdown(true);
        sendMessage(msg, "FwgThread.canWeExpectFileDeletes(): Shutting down "+
                    "threads for operation=" +
                    Operations.getOperationText(operation));
      }
    }

    return false;
  }


  protected boolean canWeGetMoreDirectories(String[] msg)
  {
    if (fwg.anchor.anyMoreDirectories() == 0)
    {
      /* No more files to obtain. Can we expect any? */
      if (expect_rmdirs)
        return true;

      synchronized (fwg)
      {
        if (!SlaveWorker.work.format_run && !fwg.getShutdown())
        {
          fwg.setShutdown(true);
          sendMessage(msg, "FwgThread.canWeGetMoreDirectories(): Shutting down "+
                      "threads for operation=" +
                      Operations.getOperationText(operation));
        }
      }

      return false;
    }

    /* Maybe we have enough potential directories, but is anyone creating new ones? */
    if (fwg.anchor.getExistingDirCount() == 0)
    {
      if (!SlaveWorker.canWeExpectDirectoryCreates())
      {
        synchronized (fwg)
        {
          if (!SlaveWorker.work.format_run && !fwg.getShutdown())
          {
            fwg.setShutdown(true);
            sendMessage(msg, "FwgThread.canWeGetMoreDirectories(): Shutting down "+
                        "threads for operation=" +
                        Operations.getOperationText(operation));
          }
        }
        return false;
      }
    }

    return true;
  }

  private void sendMessage(String[] msg, String txt)
  {
    Vector messages = new Vector(10, 0);

    for (int i = 0; i < msg.length; i++)
      messages.add(msg[i]);

    messages.add(txt);
    messages.add("");

    for (int i = 0; i < messages.size(); i++)
      common.ptod(messages.elementAt(i));

    SlaveJvm.sendMessageToConsole(messages);
  }


  protected void block(int reason)
  {
    block(reason, "");
  }

  protected void block(int reason, String txt)
  {
    //if (reason == Blocked.FILE_MAY_NOT_EXIST)
    //  common.where(8);

    //Trace.trace("c" + getName() + " " + tn.task_number + " " + reason);

    consecutive_blocks++;
    last_block = reason;
    fwg.blocked.count(reason);

    //common.ptod("block: " + txt + " " + Blocked.getLabel(reason));

    //if (reason == Blocked.FILE_MAY_NOT_EXIST)
    //  common.where(8);

    /* In a format, is there ever any need to sleep? */

    /* Was 100 usecs; with too many blocks, does this cause thrashing? */
    /* maybe do a different sleep for certain blocks, like mkdir? */

    /* Format restart and format for 'fortotal' does not need to sleep: */
    // Knowing how long to sleep is rather difficult. If I sleep too long
    // we waste opportunity to look for more work, sleeping too short may
    // lead to wasting cycles.
    // 200 usecs was too long, because in certain runs that started with a lot
    // of 'find an existing file' blocks wasted a lot of elapsed time.
    // 10 usecs was too much (got too fast to 10,000 blocks), so I added
    // an extra reason check.
    // Also, since the 10,000 consecutive block was mainly for debugging
    // purposes, why not increae it to 100,000???
    if (reason == Blocked.FILE_MUST_EXIST &&
        (fwg.getOperation() == Operations.COPY ||
         fwg.getOperation() == Operations.MOVE))
      return;

    /* 'operation=delete' that is restarted has a valid reason for this many blocks: */
    /* Worse: I did a trainload of sleeps before I died..... */
    /* But what if there is a real mixed workload going on e.g. create+delete? */
    /* I should put in a check like 'am I the only one? */
    /* How about sleeping every 'n'? or have the caller decide to sleep? */
    if (reason == Blocked.FILE_MUST_EXIST &&
        (fwg.getOperation() == Operations.DELETE))
      return;

    //if (format && reason == Blocked.FILE_MAY_NOT_EXIST)
    //  common.where(8);

    if (format)
      return;

    common.sleep_some_usecs(200);

    /* With huge amounts of directories we run into BLOCK_KILL: */
    if (format && reason == Blocked.DIR_EXISTS)
      return;
    if (format && reason == Blocked.MISSING_PARENT)
      return;
    if (format && reason == Blocked.PARENT_DIR_BUSY)
      return;

    /* Abort after 10,000 consecutive blocks, except when we are doing */
    /* a format restart (we could find 10,000 files already complete): */
    if (consecutive_blocks > BLOCK_KILL)
    {
      if (format && reason == Blocked.FILE_IS_FULL)
        return;
      if (format && reason == Blocked.FILE_MAY_NOT_EXIST)
        return;
      if (format && reason == Blocked.FILE_BUSY)
        return;

      synchronized(common.ptod_lock)
      {
        double duration = System.currentTimeMillis() - last_ok_request;

        Blocked.printTrace();
        Blocked.printAndResetCounters();

        /* This Vector is here to get away from ptod_lock: */
        Vector msg = new Vector(16);

        for (Directory dir : fwg.anchor.getDirList())
        {
          if (dir.isBusyNoSync())
          {
            msg.add(String.format("dir=%s,busy=%b", dir.getFullName(), dir.isBusyNoSync()));
            //msg.add(dir.last_busy);
          }
        }

        for (FileEntry fe : fwg.anchor.getFileList())
        {
          if (fe.isBusy())
            msg.add(String.format("file=%s,busy=%b", fe.getFullName(), fe.isBusy()));
        }


        msg.add("Thread: " + Thread.currentThread().getName());
        msg.add("");
        msg.add("last_ok_request: " + new Date(last_ok_request));
        msg.add(Format.f("Duration: %.2f seconds", duration / 1000.));
        msg.add("consecutive_blocks: " + consecutive_blocks);
        msg.add("last_block:         " + Blocked.getLabel(last_block));
        msg.add("operation:          " + Operations.getOperationText(operation));
        if (txt.length() > 0)
          msg.add("msg: " + txt);
        msg.add("");
        msg.add("Do you maybe have more threads running than that you have ");
        msg.add("files and therefore some threads ultimately give up after " +
                BLOCK_KILL + " tries?");
        SlaveJvm.sendMessageToConsole(msg);
        common.failure("Too many thread blocks");
      }
    }
  }
}
