/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.tez;

import static org.apache.tez.dag.api.client.DAGStatus.State.RUNNING;
import static org.fusesource.jansi.Ansi.ansi;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.counters.LlapIOCounters;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.fusesource.jansi.Ansi;

import com.google.common.base.Preconditions;

/**
 * TezJobMonitor keeps track of a tez job while it's being executed. It will
 * print status to the console and retrieve final status of the job after
 * completion.
 */
public class TezJobMonitor {

  private static final String CLASS_NAME = TezJobMonitor.class.getName();

  private static final int COLUMN_1_WIDTH = 16;
  private static final int SEPARATOR_WIDTH = InPlaceUpdates.MIN_TERMINAL_WIDTH;
  private static final int FILE_HEADER_SEPARATOR_WIDTH = InPlaceUpdates.MIN_TERMINAL_WIDTH + 34;
  private static final String SEPARATOR = new String(new char[SEPARATOR_WIDTH]).replace("\0", "-");
  private static final String FILE_HEADER_SEPARATOR =
      new String(new char[FILE_HEADER_SEPARATOR_WIDTH]).replace("\0", "-");
  private static final String QUERY_EXEC_SUMMARY_HEADER = "Query Execution Summary";
  private static final String TASK_SUMMARY_HEADER = "Task Execution Summary";
  private static final String LLAP_IO_SUMMARY_HEADER = "LLAP IO Summary";

  // keep this within 80 chars width. If more columns needs to be added then update min terminal
  // width requirement and SEPARATOR width accordingly
  private static final String HEADER_FORMAT = "%16s%10s %13s  %5s  %9s  %7s  %7s  %6s  %6s  ";
  private static final String VERTEX_FORMAT = "%-16s%10s %13s  %5s  %9s  %7s  %7s  %6s  %6s  ";
  private static final String FOOTER_FORMAT = "%-15s  %-30s %-4s  %-25s";
  private static final String HEADER = String.format(HEADER_FORMAT,
      "VERTICES", "MODE", "STATUS", "TOTAL", "COMPLETED", "RUNNING", "PENDING", "FAILED", "KILLED");

  // method and dag summary format
  private static final String SUMMARY_HEADER_FORMAT = "%10s %14s %13s %12s %14s %15s";
  private static final String SUMMARY_HEADER = String.format(SUMMARY_HEADER_FORMAT,
      "VERTICES", "DURATION(ms)", "CPU_TIME(ms)", "GC_TIME(ms)", "INPUT_RECORDS", "OUTPUT_RECORDS");

  // used when I/O redirection is used
  private static final String FILE_HEADER_FORMAT = "%10s %12s %16s %13s %14s %13s %12s %14s %15s";
  private static final String FILE_HEADER = String.format(FILE_HEADER_FORMAT,
      "VERTICES", "TOTAL_TASKS", "FAILED_ATTEMPTS", "KILLED_TASKS", "DURATION(ms)",
      "CPU_TIME(ms)", "GC_TIME(ms)", "INPUT_RECORDS", "OUTPUT_RECORDS");

  // LLAP counters
  private static final String LLAP_SUMMARY_HEADER_FORMAT = "%10s %9s %9s %10s %9s %10s %11s %8s %9s";
  private static final String LLAP_SUMMARY_HEADER = String.format(LLAP_SUMMARY_HEADER_FORMAT,
      "VERTICES", "ROWGROUPS", "META_HIT", "META_MISS", "DATA_HIT", "DATA_MISS",
      "ALLOCATION", "USED", "TOTAL_IO");

  // Methods summary
  private static final String OPERATION_SUMMARY = "%-35s %9s";
  private static final String OPERATION = "OPERATION";
  private static final String DURATION = "DURATION";

  // in-place progress update related variables
  private int lines;
  private final PrintStream out;

  private transient LogHelper console;
  private final PerfLogger perfLogger = SessionState.getPerfLogger();
  private final int checkInterval = 200;
  private final int maxRetryInterval = 2500;
  private final int printInterval = 3000;
  private final int progressBarChars = 30;
  private long lastPrintTime;
  private Set<String> completed;

  /* Pretty print the values */
  private final NumberFormat secondsFormat;
  private final NumberFormat commaFormat;
  private static final List<DAGClient> shutdownList;
  private final Map<String, BaseWork> workMap;

  private StringBuffer diagnostics;

  static {
    shutdownList = new LinkedList<DAGClient>();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        TezJobMonitor.killRunningJobs();
        try {
          TezSessionPoolManager.getInstance().closeNonDefaultSessions(false);
        } catch (Exception e) {
          // ignore
        }
      }
    });
  }

  public static void initShutdownHook() {
    Preconditions.checkNotNull(shutdownList,
        "Shutdown hook was not properly initialized");
  }

  public TezJobMonitor(Map<String, BaseWork> workMap) {
    this.workMap = workMap;
    console = SessionState.getConsole();
    secondsFormat = new DecimalFormat("#0.00");
    commaFormat = NumberFormat.getNumberInstance(Locale.US);
    // all progress updates are written to info stream and log file. In-place updates can only be
    // done to info stream (console)
    out = console.getInfoStream();
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Erases the current line and prints the given line.
   * @param line - line to print
   */
  public void reprintLine(String line) {
    InPlaceUpdates.reprintLine(out, line);
    lines++;
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Erases the current line and prints the given line with the specified color.
   * @param line - line to print
   * @param color - color for the line
   */
  public void reprintLineWithColorAsBold(String line, Ansi.Color color) {
    out.print(ansi().eraseLine(Ansi.Erase.ALL).fg(color).bold().a(line).a('\n').boldOff().reset()
        .toString());
    out.flush();
    lines++;
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Erases the current line and prints the given multiline. Make sure the specified line is not
   * terminated by linebreak.
   * @param line - line to print
   */
  public void reprintMultiLine(String line) {
    int numLines = line.split("\r\n|\r|\n").length;
    out.print(ansi().eraseLine(Ansi.Erase.ALL).a(line).a('\n').toString());
    out.flush();
    lines += numLines;
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Repositions the cursor back to line 0.
   */
  public void repositionCursor() {
    if (lines > 0) {
      out.print(ansi().cursorUp(lines).toString());
      out.flush();
      lines = 0;
    }
  }

  /**
   * monitorExecution handles status printing, failures during execution and final status retrieval.
   *
   * @param dagClient client that was used to kick off the job
   * @param conf configuration file for this operation
   * @return int 0 - success, 1 - killed, 2 - failed
   */
  public int monitorExecution(final DAGClient dagClient, HiveConf conf,
      DAG dag, Context ctx) throws InterruptedException {
    long monitorStartTime = System.currentTimeMillis();
    DAGStatus status = null;
    completed = new HashSet<String>();
    diagnostics = new StringBuffer();

    boolean running = false;
    boolean done = false;
    boolean success = false;
    int failedCounter = 0;
    int rc = 0;
    DAGStatus.State lastState = null;
    String lastReport = null;
    Set<StatusGetOpts> opts = new HashSet<StatusGetOpts>();
    long startTime = 0;
    boolean isProfileEnabled = HiveConf.getBoolVar(conf, HiveConf.ConfVars.TEZ_EXEC_SUMMARY) ||
        Utilities.isPerfOrAboveLogging(conf);
    boolean llapIoEnabled = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_IO_ENABLED, false);

    boolean inPlaceEligible = InPlaceUpdates.inPlaceEligible(conf);
    synchronized(shutdownList) {
      shutdownList.add(dagClient);
    }
    console.printInfo("\n");
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_DAG);
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_SUBMIT_TO_RUNNING);
    Map<String, Progress> progressMap = null;
    while (true) {

      try {
        if (ctx != null) {
          ctx.checkHeartbeaterLockException();
        }

        status = dagClient.getDAGStatus(opts, checkInterval);
        progressMap = status.getVertexProgress();
        DAGStatus.State state = status.getState();

        if (state != lastState || state == RUNNING) {
          lastState = state;

          switch (state) {
          case SUBMITTED:
            console.printInfo("Status: Submitted");
            break;
          case INITING:
            console.printInfo("Status: Initializing");
            startTime = System.currentTimeMillis();
            break;
          case RUNNING:
            if (!running) {
              perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_SUBMIT_TO_RUNNING);
              console.printInfo("Status: Running (" + dagClient.getExecutionContext() + ")\n");
              startTime = System.currentTimeMillis();
              running = true;
            }

            if (inPlaceEligible) {
              printStatusInPlace(progressMap, startTime, false, dagClient);
              // log the progress report to log file as well
              lastReport = logStatus(progressMap, lastReport, console);
            } else {
              lastReport = printStatus(progressMap, lastReport, console);
            }
            break;
          case SUCCEEDED:
            if (!running) {
              startTime = monitorStartTime;
            }
            if (inPlaceEligible) {
              printStatusInPlace(progressMap, startTime, false, dagClient);
              // log the progress report to log file as well
              lastReport = logStatus(progressMap, lastReport, console);
            } else {
              lastReport = printStatus(progressMap, lastReport, console);
            }
            success = true;
            running = false;
            done = true;
            break;
          case KILLED:
            if (!running) {
              startTime = monitorStartTime;
            }
            if (inPlaceEligible) {
              printStatusInPlace(progressMap, startTime, true, dagClient);
              // log the progress report to log file as well
              lastReport = logStatus(progressMap, lastReport, console);
            }
            console.printInfo("Status: Killed");
            running = false;
            done = true;
            rc = 1;
            break;
          case FAILED:
          case ERROR:
            if (!running) {
              startTime = monitorStartTime;
            }
            if (inPlaceEligible) {
              printStatusInPlace(progressMap, startTime, true, dagClient);
              // log the progress report to log file as well
              lastReport = logStatus(progressMap, lastReport, console);
            }
            console.printError("Status: Failed");
            running = false;
            done = true;
            rc = 2;
            break;
          }
        }
      } catch (Exception e) {
        console.printInfo("Exception: " + e.getMessage());
        boolean isInterrupted = hasInterruptedException(e);
        if (isInterrupted || (++failedCounter % maxRetryInterval / checkInterval == 0)) {
          try {
            console.printInfo("Killing DAG...");
            dagClient.tryKillDAG();
          } catch (IOException io) {
            // best effort
          } catch (TezException te) {
            // best effort
          }
          e.printStackTrace();
          console.printError("Execution has failed.");
          rc = 1;
          done = true;
        } else {
          console.printInfo("Retrying...");
        }
      } finally {
        if (done) {
          if (rc != 0 && status != null) {
            for (String diag : status.getDiagnostics()) {
              console.printError(diag);
              diagnostics.append(diag);
            }
          }
          synchronized(shutdownList) {
            shutdownList.remove(dagClient);
          }
          break;
        }
      }
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_RUN_DAG);

    if (isProfileEnabled && success && progressMap != null) {

      double duration = (System.currentTimeMillis() - startTime) / 1000.0;
      console.printInfo("Status: DAG finished successfully in "
          + String.format("%.2f seconds", duration));
      console.printInfo("");

      console.printInfo(QUERY_EXEC_SUMMARY_HEADER);
      printQueryExecutionBreakDown();
      console.printInfo(SEPARATOR);
      console.printInfo("");

      console.printInfo(TASK_SUMMARY_HEADER);
      printDagSummary(progressMap, console, dagClient, conf, dag, inPlaceEligible);
      if (inPlaceEligible) {
        console.printInfo(SEPARATOR);
      } else {
        console.printInfo(FILE_HEADER_SEPARATOR);
      }

      if (llapIoEnabled) {
        console.printInfo("");
        console.printInfo(LLAP_IO_SUMMARY_HEADER);
        printLlapIOSummary(progressMap, console, dagClient);
        console.printInfo(SEPARATOR);
      }

      console.printInfo("");
    }

    return rc;
  }

  private static boolean hasInterruptedException(Throwable e) {
    // Hadoop IPC wraps InterruptedException. GRRR.
    while (e != null) {
      if (e instanceof InterruptedException || e instanceof InterruptedIOException) {
        return true;
      }
      e = e.getCause();
    }
    return false;
  }

  /**
   * killRunningJobs tries to terminate execution of all
   * currently running tez queries. No guarantees, best effort only.
   */
  public static void killRunningJobs() {
    synchronized (shutdownList) {
      for (DAGClient c : shutdownList) {
        try {
          System.err.println("Trying to shutdown DAG");
          c.tryKillDAG();
        } catch (Exception e) {
          // ignore
        }
      }
    }
  }

  private static long getCounterValueByGroupName(TezCounters vertexCounters,
      String groupNamePattern,
      String counterName) {
    TezCounter tezCounter = vertexCounters.getGroup(groupNamePattern).findCounter(counterName);
    return (tezCounter == null) ? 0 : tezCounter.getValue();
  }

  private void printQueryExecutionBreakDown() {

    /* Build the method summary header */
    String execBreakdownHeader = String.format(OPERATION_SUMMARY, OPERATION, DURATION);
    console.printInfo(SEPARATOR);
    reprintLineWithColorAsBold(execBreakdownHeader, Ansi.Color.CYAN);
    console.printInfo(SEPARATOR);

    // parse, analyze, optimize and compile
    long compile = perfLogger.getEndTime(PerfLogger.COMPILE) -
        perfLogger.getStartTime(PerfLogger.COMPILE);
    console.printInfo(String.format(OPERATION_SUMMARY, "Compile Query",
        secondsFormat.format(compile / 1000.0) + "s"));

    // prepare plan for submission (building DAG, adding resources, creating scratch dirs etc.)
    long totalDAGPrep = perfLogger.getStartTime(PerfLogger.TEZ_SUBMIT_DAG) -
        perfLogger.getEndTime(PerfLogger.COMPILE);
    console.printInfo(String.format(OPERATION_SUMMARY, "Prepare Plan",
        secondsFormat.format(totalDAGPrep / 1000.0) + "s"));

    // submit to accept dag (if session is closed, this will include re-opening of session time,
    // localizing files for AM, submitting DAG)
    long submitToAccept = perfLogger.getStartTime(PerfLogger.TEZ_RUN_DAG) -
        perfLogger.getStartTime(PerfLogger.TEZ_SUBMIT_DAG);
    console.printInfo(String.format(OPERATION_SUMMARY, "Submit Plan",
        secondsFormat.format(submitToAccept / 1000.0) + "s"));

    // accept to start dag (schedule wait time, resource wait time etc.)
    long acceptToStart = perfLogger.getDuration(PerfLogger.TEZ_SUBMIT_TO_RUNNING);
    console.printInfo(String.format(OPERATION_SUMMARY, "Start DAG",
        secondsFormat.format(acceptToStart / 1000.0) + "s"));

    // time to actually run the dag (actual dag runtime)
    final long startToEnd;
    if (acceptToStart == 0) {
      startToEnd = perfLogger.getDuration(PerfLogger.TEZ_RUN_DAG);
    } else {
      startToEnd = perfLogger.getEndTime(PerfLogger.TEZ_RUN_DAG) -
          perfLogger.getEndTime(PerfLogger.TEZ_SUBMIT_TO_RUNNING);
    }
    console.printInfo(String.format(OPERATION_SUMMARY, "Run DAG",
        secondsFormat.format(startToEnd / 1000.0) + "s"));

  }

  private void printDagSummary(Map<String, Progress> progressMap, LogHelper console,
      DAGClient dagClient, HiveConf conf, DAG dag, final boolean inPlaceEligible) {

    /* Strings for headers and counters */
    String hiveCountersGroup = HiveConf.getVar(conf, HiveConf.ConfVars.HIVECOUNTERGROUP);
    Set<StatusGetOpts> statusGetOpts = EnumSet.of(StatusGetOpts.GET_COUNTERS);
    TezCounters hiveCounters = null;
    try {
      hiveCounters = dagClient.getDAGStatus(statusGetOpts).getDAGCounters();
    } catch (IOException e) {
      // best attempt, shouldn't really kill DAG for this
    } catch (TezException e) {
      // best attempt, shouldn't really kill DAG for this
    }

    /* If the counters are missing there is no point trying to print progress */
    if (hiveCounters == null) {
      return;
    }

    /* Print the per Vertex summary */
    if (inPlaceEligible) {
      console.printInfo(SEPARATOR);
      reprintLineWithColorAsBold(SUMMARY_HEADER, Ansi.Color.CYAN);
      console.printInfo(SEPARATOR);
    } else {
      console.printInfo(FILE_HEADER_SEPARATOR);
      reprintLineWithColorAsBold(FILE_HEADER, Ansi.Color.CYAN);
      console.printInfo(FILE_HEADER_SEPARATOR);
    }
    SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
    Set<StatusGetOpts> statusOptions = new HashSet<StatusGetOpts>(1);
    statusOptions.add(StatusGetOpts.GET_COUNTERS);
    for (String vertexName : keys) {
      Progress progress = progressMap.get(vertexName);
      if (progress != null) {
        final int totalTasks = progress.getTotalTaskCount();
        final int failedTaskAttempts = progress.getFailedTaskAttemptCount();
        final int killedTaskAttempts = progress.getKilledTaskAttemptCount();
        final double duration = perfLogger.getDuration(PerfLogger.TEZ_RUN_VERTEX + vertexName);
        VertexStatus vertexStatus = null;
        try {
          vertexStatus = dagClient.getVertexStatus(vertexName, statusOptions);
        } catch (IOException e) {
          // best attempt, shouldn't really kill DAG for this
        } catch (TezException e) {
          // best attempt, shouldn't really kill DAG for this
        }

        if (vertexStatus == null) {
          continue;
        }

        Vertex currentVertex = dag.getVertex(vertexName);
        List<Vertex> inputVerticesList = currentVertex.getInputVertices();
        long hiveInputRecordsFromOtherVertices = 0;
        if (inputVerticesList.size() > 0) {

          for (Vertex inputVertex : inputVerticesList) {
            String inputVertexName = inputVertex.getName();
            hiveInputRecordsFromOtherVertices += getCounterValueByGroupName(hiveCounters,
                hiveCountersGroup, String.format("%s_",
                    ReduceSinkOperator.Counter.RECORDS_OUT_INTERMEDIATE.toString()) +
                    inputVertexName.replace(" ", "_"));

            hiveInputRecordsFromOtherVertices += getCounterValueByGroupName(hiveCounters,
                hiveCountersGroup, String.format("%s_",
                    FileSinkOperator.Counter.RECORDS_OUT.toString()) +
                    inputVertexName.replace(" ", "_"));
          }
        }

      /*
       * Get the CPU & GC
       *
       * counters org.apache.tez.common.counters.TaskCounter
       *  GC_TIME_MILLIS=37712
       *  CPU_MILLISECONDS=2774230
       */
        final TezCounters vertexCounters = vertexStatus.getVertexCounters();
        final double cpuTimeMillis = getCounterValueByGroupName(vertexCounters,
            TaskCounter.class.getName(),
            TaskCounter.CPU_MILLISECONDS.name());

        final double gcTimeMillis = getCounterValueByGroupName(vertexCounters,
            TaskCounter.class.getName(),
            TaskCounter.GC_TIME_MILLIS.name());

      /*
       * Get the HIVE counters
       *
       * HIVE
       *  CREATED_FILES=1
       *  DESERIALIZE_ERRORS=0
       *  RECORDS_IN_Map_1=550076554
       *  RECORDS_OUT_INTERMEDIATE_Map_1=854987
       *  RECORDS_OUT_Reducer_2=1
       */

        final long hiveInputRecords =
            getCounterValueByGroupName(
                hiveCounters,
                hiveCountersGroup,
                String.format("%s_", MapOperator.Counter.RECORDS_IN.toString())
                    + vertexName.replace(" ", "_"))
                + hiveInputRecordsFromOtherVertices;
        final long hiveOutputIntermediateRecords =
            getCounterValueByGroupName(
                hiveCounters,
                hiveCountersGroup,
                String.format("%s_", ReduceSinkOperator.Counter.RECORDS_OUT_INTERMEDIATE.toString())
                    + vertexName.replace(" ", "_"));
        final long hiveOutputRecords =
            getCounterValueByGroupName(
                hiveCounters,
                hiveCountersGroup,
                String.format("%s_", FileSinkOperator.Counter.RECORDS_OUT.toString())
                    + vertexName.replace(" ", "_"))
                + hiveOutputIntermediateRecords;

        final String vertexExecutionStats;
        if (inPlaceEligible) {
          vertexExecutionStats = String.format(SUMMARY_HEADER_FORMAT,
              vertexName,
              secondsFormat.format((duration)),
              commaFormat.format(cpuTimeMillis),
              commaFormat.format(gcTimeMillis),
              commaFormat.format(hiveInputRecords),
              commaFormat.format(hiveOutputRecords));
        } else {
          vertexExecutionStats = String.format(FILE_HEADER_FORMAT,
              vertexName,
              totalTasks,
              failedTaskAttempts,
              killedTaskAttempts,
              secondsFormat.format((duration)),
              commaFormat.format(cpuTimeMillis),
              commaFormat.format(gcTimeMillis),
              commaFormat.format(hiveInputRecords),
              commaFormat.format(hiveOutputRecords));
        }
        console.printInfo(vertexExecutionStats);
      }
    }
  }


  private String humanReadableByteCount(long bytes) {
    int unit = 1000; // use binary units instead?
    if (bytes < unit) {
      return bytes + "B";
    }
    int exp = (int) (Math.log(bytes) / Math.log(unit));
    String suffix = "KMGTPE".charAt(exp-1) + "";
    return String.format("%.2f%sB", bytes / Math.pow(unit, exp), suffix);
  }

  private void printLlapIOSummary(Map<String, Progress> progressMap, LogHelper console,
      DAGClient dagClient) {
    SortedSet<String> keys = new TreeSet<>(progressMap.keySet());
    Set<StatusGetOpts> statusOptions = new HashSet<>(1);
    statusOptions.add(StatusGetOpts.GET_COUNTERS);
    boolean first = false;
    String counterGroup = LlapIOCounters.class.getName();
    for (String vertexName : keys) {
      // Reducers do not benefit from LLAP IO so no point in printing
      if (vertexName.startsWith("Reducer")) {
        continue;
      }
      TezCounters vertexCounters = null;
      try {
        vertexCounters = dagClient.getVertexStatus(vertexName, statusOptions)
            .getVertexCounters();
      } catch (IOException e) {
        // best attempt, shouldn't really kill DAG for this
      } catch (TezException e) {
        // best attempt, shouldn't really kill DAG for this
      }
      if (vertexCounters != null) {
        final long selectedRowgroups = getCounterValueByGroupName(vertexCounters,
            counterGroup, LlapIOCounters.SELECTED_ROWGROUPS.name());
        final long metadataCacheHit = getCounterValueByGroupName(vertexCounters,
            counterGroup, LlapIOCounters.METADATA_CACHE_HIT.name());
        final long metadataCacheMiss = getCounterValueByGroupName(vertexCounters,
            counterGroup, LlapIOCounters.METADATA_CACHE_MISS.name());
        final long cacheHitBytes = getCounterValueByGroupName(vertexCounters,
            counterGroup, LlapIOCounters.CACHE_HIT_BYTES.name());
        final long cacheMissBytes = getCounterValueByGroupName(vertexCounters,
            counterGroup, LlapIOCounters.CACHE_MISS_BYTES.name());
        final long allocatedBytes = getCounterValueByGroupName(vertexCounters,
            counterGroup, LlapIOCounters.ALLOCATED_BYTES.name());
        final long allocatedUsedBytes = getCounterValueByGroupName(vertexCounters,
            counterGroup, LlapIOCounters.ALLOCATED_USED_BYTES.name());
        final long totalIoTime = getCounterValueByGroupName(vertexCounters,
            counterGroup, LlapIOCounters.TOTAL_IO_TIME_NS.name());

        if (!first) {
          console.printInfo(SEPARATOR);
          reprintLineWithColorAsBold(LLAP_SUMMARY_HEADER, Ansi.Color.CYAN);
          console.printInfo(SEPARATOR);
          first = true;
        }

        String queryFragmentStats = String.format(LLAP_SUMMARY_HEADER_FORMAT,
            vertexName,
            selectedRowgroups,
            metadataCacheHit,
            metadataCacheMiss,
            humanReadableByteCount(cacheHitBytes),
            humanReadableByteCount(cacheMissBytes),
            humanReadableByteCount(allocatedBytes),
            humanReadableByteCount(allocatedUsedBytes),
            secondsFormat.format(totalIoTime / 1000_000_000.0) + "s");
        console.printInfo(queryFragmentStats);
      }
    }
  }

  private void printStatusInPlace(Map<String, Progress> progressMap, long startTime,
      boolean vextexStatusFromAM, DAGClient dagClient) {
    StringBuilder reportBuffer = new StringBuilder();
    int sumComplete = 0;
    int sumTotal = 0;

    // position the cursor to line 0
    repositionCursor();

    // print header
    // -------------------------------------------------------------------------------
    //         VERTICES     STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
    // -------------------------------------------------------------------------------
    reprintLine(SEPARATOR);
    reprintLineWithColorAsBold(HEADER, Ansi.Color.CYAN);
    reprintLine(SEPARATOR);

    SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
    int idx = 0;
    int maxKeys = keys.size();
    for (String s : keys) {
      idx++;
      Progress progress = progressMap.get(s);
      final int complete = progress.getSucceededTaskCount();
      final int total = progress.getTotalTaskCount();
      final int running = progress.getRunningTaskCount();
      final int failed = progress.getFailedTaskAttemptCount();
      final int pending = progress.getTotalTaskCount() - progress.getSucceededTaskCount() -
          progress.getRunningTaskCount();
      final int killed = progress.getKilledTaskAttemptCount();

      // To get vertex status we can use DAGClient.getVertexStatus(), but it will be expensive to
      // get status from AM for every refresh of the UI. Lets infer the state from task counts.
      // Only if DAG is FAILED or KILLED the vertex status is fetched from AM.
      VertexStatus.State vertexState = VertexStatus.State.INITIALIZING;

      // INITED state
      if (total > 0) {
        vertexState = VertexStatus.State.INITED;
        sumComplete += complete;
        sumTotal += total;
      }

      // RUNNING state
      if (complete < total && (complete > 0 || running > 0 || failed > 0)) {
        vertexState = VertexStatus.State.RUNNING;
        if (!perfLogger.startTimeHasMethod(PerfLogger.TEZ_RUN_VERTEX + s)) {
          perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
        }
      }

      // SUCCEEDED state
      if (complete == total) {
        vertexState = VertexStatus.State.SUCCEEDED;
        if (!completed.contains(s)) {
          completed.add(s);

            /* We may have missed the start of the vertex
             * due to the 3 seconds interval
             */
          if (!perfLogger.startTimeHasMethod(PerfLogger.TEZ_RUN_VERTEX + s)) {
            perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
          }

          perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
        }
      }

      // DAG might have been killed, lets try to get vertex state from AM before dying
      // KILLED or FAILED state
      if (vextexStatusFromAM) {
        VertexStatus vertexStatus = null;
        try {
          vertexStatus = dagClient.getVertexStatus(s, null);
        } catch (IOException e) {
          // best attempt, shouldn't really kill DAG for this
        } catch (TezException e) {
          // best attempt, shouldn't really kill DAG for this
        }
        if (vertexStatus != null) {
          vertexState = vertexStatus.getState();
        }
      }

      // Map 1 .......... container  SUCCEEDED      7          7        0        0       0       0
      String nameWithProgress = getNameWithProgress(s, complete, total);
      String mode = getMode(s, workMap);
      String vertexStr = String.format(VERTEX_FORMAT,
          nameWithProgress,
          mode,
          vertexState.toString(),
          total,
          complete,
          running,
          pending,
          failed,
          killed);
      reportBuffer.append(vertexStr);
      if (idx != maxKeys) {
        reportBuffer.append("\n");
      }
    }

    reprintMultiLine(reportBuffer.toString());

    // -------------------------------------------------------------------------------
    // VERTICES: 03/04            [=================>>-----] 86%  ELAPSED TIME: 1.71 s
    // -------------------------------------------------------------------------------
    reprintLine(SEPARATOR);
    final float progress = (sumTotal == 0) ? 0.0f : (float) sumComplete / (float) sumTotal;
    String footer = getFooter(keys.size(), completed.size(), progress, startTime);
    reprintLineWithColorAsBold(footer, Ansi.Color.RED);
    reprintLine(SEPARATOR);
  }

  private String getMode(String name, Map<String, BaseWork> workMap) {
    String mode = "container";
    BaseWork work = workMap.get(name);
    if (work != null) {
      // uber > llap > container
      if (work.getUberMode()) {
        mode = "uber";
      } else if (work.getLlapMode()) {
        mode = "llap";
      } else {
        mode = "container";
      }
    }
    return mode;
  }

  // Map 1 ..........
  private String getNameWithProgress(String s, int complete, int total) {
    String result = "";
    if (s != null) {
      float percent = total == 0 ? 0.0f : (float) complete / (float) total;
      // lets use the remaining space in column 1 as progress bar
      int spaceRemaining = COLUMN_1_WIDTH - s.length() - 1;
      String trimmedVName = s;

      // if the vertex name is longer than column 1 width, trim it down
      // "Tez Merge File Work" will become "Tez Merge File.."
      if (s.length() > COLUMN_1_WIDTH) {
        trimmedVName = s.substring(0, COLUMN_1_WIDTH - 1);
        trimmedVName = trimmedVName + "..";
      }

      result = trimmedVName + " ";
      int toFill = (int) (spaceRemaining * percent);
      for (int i = 0; i < toFill; i++) {
        result += ".";
      }
    }
    return result;
  }

  // VERTICES: 03/04            [==================>>-----] 86%  ELAPSED TIME: 1.71 s
  private String getFooter(int keySize, int completedSize, float progress, long startTime) {
    String verticesSummary = String.format("VERTICES: %02d/%02d", completedSize, keySize);
    String progressBar = getInPlaceProgressBar(progress);
    final int progressPercent = (int) (progress * 100);
    String progressStr = "" + progressPercent + "%";
    float et = (float) (System.currentTimeMillis() - startTime) / (float) 1000;
    String elapsedTime = "ELAPSED TIME: " + secondsFormat.format(et) + " s";
    String footer = String.format(FOOTER_FORMAT,
        verticesSummary, progressBar, progressStr, elapsedTime);
    return footer;
  }

  // [==================>>-----]
  private String getInPlaceProgressBar(float percent) {
    StringBuilder bar = new StringBuilder("[");
    int remainingChars = progressBarChars - 4;
    int completed = (int) (remainingChars * percent);
    int pending = remainingChars - completed;
    for (int i = 0; i < completed; i++) {
      bar.append("=");
    }
    bar.append(">>");
    for (int i = 0; i < pending; i++) {
      bar.append("-");
    }
    bar.append("]");
    return bar.toString();
  }

  private String printStatus(Map<String, Progress> progressMap, String lastReport, LogHelper console) {
    String report = getReport(progressMap);
    if (!report.equals(lastReport) || System.currentTimeMillis() >= lastPrintTime + printInterval) {
      console.printInfo(report);
      lastPrintTime = System.currentTimeMillis();
    }
    return report;
  }

  private String logStatus(Map<String, Progress> progressMap, String lastReport, LogHelper console) {
    String report = getReport(progressMap);
    if (!report.equals(lastReport) || System.currentTimeMillis() >= lastPrintTime + printInterval) {
      console.logInfo(report);
      lastPrintTime = System.currentTimeMillis();
    }
    return report;
  }

  private String getReport(Map<String, Progress> progressMap) {
    StringBuilder reportBuffer = new StringBuilder();

    SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
    for (String s: keys) {
      Progress progress = progressMap.get(s);
      final int complete = progress.getSucceededTaskCount();
      final int total = progress.getTotalTaskCount();
      final int running = progress.getRunningTaskCount();
      final int failed = progress.getFailedTaskAttemptCount();
      if (total <= 0) {
        reportBuffer.append(String.format("%s: -/-\t", s));
      } else {
        if (complete == total && !completed.contains(s)) {
          completed.add(s);

          /*
           * We may have missed the start of the vertex due to the 3 seconds interval
           */
          if (!perfLogger.startTimeHasMethod(PerfLogger.TEZ_RUN_VERTEX + s)) {
            perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
          }

          perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
        }
        if(complete < total && (complete > 0 || running > 0 || failed > 0)) {

          if (!perfLogger.startTimeHasMethod(PerfLogger.TEZ_RUN_VERTEX + s)) {
            perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
          }

          /* vertex is started, but not complete */
          if (failed > 0) {
            reportBuffer.append(String.format("%s: %d(+%d,-%d)/%d\t", s, complete, running, failed, total));
          } else {
            reportBuffer.append(String.format("%s: %d(+%d)/%d\t", s, complete, running, total));
          }
        } else {
          /* vertex is waiting for input/slots or complete */
          if (failed > 0) {
            /* tasks finished but some failed */
            reportBuffer.append(String.format("%s: %d(-%d)/%d\t", s, complete, failed, total));
          } else {
            reportBuffer.append(String.format("%s: %d/%d\t", s, complete, total));
          }
        }
      }
    }

    return reportBuffer.toString();
  }

  public String getDiagnostics() {
    return diagnostics.toString();
  }
}
