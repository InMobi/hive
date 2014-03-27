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
package org.apache.hive.service.cli.operation;

import java.io.*;
import java.util.List;
import java.util.EnumSet;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.TaskStatus;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.thrift.TProtocolVersion;

public abstract class Operation {
  protected final HiveSession parentSession;
  private OperationState state = OperationState.INITIALIZED;
  private final OperationHandle opHandle;
  private HiveConf configuration;
  public static final Log LOG = LogFactory.getLog(Operation.class.getName());
  public static final long DEFAULT_FETCH_MAX_ROWS = 100;
  protected boolean hasResultSet;
  protected SessionState.LogHelper console;
  protected PrintStream opLogOut;
  protected PrintStream opLogErr;
  protected volatile HiveSQLException operationException;
  protected long operationStart;
  protected long operationComplete;
  protected final boolean runAsync;
  protected volatile Future<?> backgroundHandle;

  protected static final EnumSet<FetchOrientation> DEFAULT_FETCH_ORIENTATION_SET =
      EnumSet.of(FetchOrientation.FETCH_NEXT,FetchOrientation.FETCH_FIRST);

  protected Operation(HiveSession parentSession, OperationType opType, boolean runInBackground) {
    super();
    this.parentSession = parentSession;
    this.runAsync = runInBackground;
    this.opHandle = new OperationHandle(opType, parentSession.getProtocolVersion());
    console = new SessionState.LogHelper(LOG);
  }

  public Future<?> getBackgroundHandle() {
    return backgroundHandle;
  }

  protected void setBackgroundHandle(Future<?> backgroundHandle) {
    this.backgroundHandle = backgroundHandle;
  }

  public boolean shouldRunAsync() {
    return runAsync;
  }

  public void setConfiguration(HiveConf configuration) {
    this.configuration = new HiveConf(configuration);
  }

  public HiveConf getConfiguration() {
    return new HiveConf(configuration);
  }

  public HiveSession getParentSession() {
    return parentSession;
  }

  public OperationHandle getHandle() {
    return opHandle;
  }

  public TProtocolVersion getProtocolVersion() {
    return opHandle.getProtocolVersion();
  }

  public OperationType getType() {
    return opHandle.getOperationType();
  }

  public OperationStatus getStatus() {
    String taskStatus = null;
    try {
      taskStatus = getTaskStatus();
    } catch (HiveSQLException sqlException) {
      LOG.error("Error getting task status for " + opHandle.toString(), sqlException);
    }
    return new OperationStatus(state, operationException, taskStatus, operationStart, operationComplete);
  }

  public boolean hasResultSet() {
    return hasResultSet;
  }

  protected void setHasResultSet(boolean hasResultSet) {
    this.hasResultSet = hasResultSet;
    opHandle.setHasResultSet(hasResultSet);
  }

  protected final OperationState setState(OperationState newState) throws HiveSQLException {
    state.validateTransition(newState);
    this.state = newState;
    return this.state;
  }

  protected void setOperationException(HiveSQLException operationException) {
    this.operationException = operationException;
  }

  protected final void assertState(OperationState state) throws HiveSQLException {
    if (this.state != state) {
      throw new HiveSQLException("Expected state " + state + ", but found " + this.state);
    }
  }

  public boolean isRunning() {
    return OperationState.RUNNING.equals(state);
  }

  public boolean isFinished() {
    return OperationState.FINISHED.equals(state);
  }

  public boolean isCanceled() {
    return OperationState.CANCELED.equals(state);
  }

  public boolean isFailed() {
    return OperationState.ERROR.equals(state);
  }

  public abstract void run() throws HiveSQLException;

  // TODO: make this abstract and implement in subclasses.
  public void cancel() throws HiveSQLException {
    setState(OperationState.CANCELED);
    throw new UnsupportedOperationException("SQLOperation.cancel()");
  }

  public abstract void close() throws HiveSQLException;

  public abstract TableSchema getResultSetSchema() throws HiveSQLException;

  public abstract RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException;

  public RowSet getNextRowSet() throws HiveSQLException {
    return getNextRowSet(FetchOrientation.FETCH_NEXT, DEFAULT_FETCH_MAX_ROWS);
  }

  public String getTaskStatus() throws HiveSQLException {
    return null;
  }

  public void setConsole(SessionState.LogHelper log) {
    console = log;
  }

  public SessionState.LogHelper getConsole() {
    return console;
  }

  protected void openLogStreams() {
    if (parentSession.isLogRedirectionEnabled()) {
      File sessionLogDir = parentSession.getSessionLogDir();
      String operationId = opHandle.getHandleIdentifier().toString();
      try {
        opLogOut = new PrintStream(new FileOutputStream(new File(sessionLogDir, operationId + ".out")),
          true, "UTF-8");
        opLogErr = new PrintStream(new FileOutputStream(new File(sessionLogDir, operationId + ".err")),
          true, "UTF-8" );
        console.setInfoStream(opLogOut);
        console.setOutStream(opLogOut);
        console.setErrorStream(opLogErr);
        console.setChildErrStream(opLogErr);
        console.setChildOutStream(opLogOut);
      } catch (FileNotFoundException e) {
        LOG.error("Error setting up log redirection for operation " + getHandle().toString(), e);
      } catch (UnsupportedEncodingException e) {
        LOG.error("Error setting up log redirection, unable to create print stream", e);
      }

      LOG.info("Created session log files in dir " + parentSession.getSessionLogDir().getAbsolutePath()
        + " operation " + operationId);
    }
  }

  protected void closeLogStreams() {
    if (parentSession.isLogRedirectionEnabled()) {
      if (opLogOut != null) {
        opLogOut.close();
      }

      if (opLogErr != null) {
        opLogErr.close();
      }
    }
  }

  public long getOperationComplete() {
    return operationComplete;
  }

  public long getOperationStart() {
    return operationStart;
  }

  protected void markOperationStartTime() {
    operationStart = System.currentTimeMillis();
  }
  protected void markOperationCompletedTime() {
    operationComplete = System.currentTimeMillis();
  }

  /**
   * Verify if the given fetch orientation is part of the default orientation types.
   * @param orientation
   * @throws HiveSQLException
   */
  protected void validateDefaultFetchOrientation(FetchOrientation orientation)
      throws HiveSQLException {
    validateFetchOrientation(orientation, DEFAULT_FETCH_ORIENTATION_SET);
  }

  /**
   * Verify if the given fetch orientation is part of the supported orientation types.
   * @param orientation
   * @param supportedOrientations
   * @throws HiveSQLException
   */
  protected void validateFetchOrientation(FetchOrientation orientation,
      EnumSet<FetchOrientation> supportedOrientations) throws HiveSQLException {
    if (!supportedOrientations.contains(orientation)) {
      throw new HiveSQLException("The fetch type " + orientation.toString() +
          " is not supported for this resultset", "HY106");
    }
  }
}
