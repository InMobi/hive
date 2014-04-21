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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.TaskStatus;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * SQLOperation.
 *
 */
public class SQLOperation extends ExecuteStatementOperation {

  private Driver driver = null;
  private CommandProcessorResponse response;
  private TableSchema resultSchema = null;
  private Schema mResultSchema = null;
  private SerDe serde = null;
  private boolean fetchStarted = false;

  public SQLOperation(HiveSession parentSession, String statement, Map<String,
      String> confOverlay, boolean runInBackground) {
    // TODO: call setRemoteUser in ExecuteStatementOperation or higher.
    super(parentSession, statement, confOverlay, runInBackground);
    openLogStreams();
  }

  /***
   * Compile the query and extract metadata
   * @param sqlOperationConf
   * @throws HiveSQLException
   */
  public void prepare(HiveConf sqlOperationConf) throws HiveSQLException {
    setState(OperationState.RUNNING);

    try {
      driver = new Driver(sqlOperationConf, getParentSession().getUserName());
      boolean logRedirectionEnabled = getParentSession().isLogRedirectionEnabled();
      driver.setLogRedirecionEnabled(logRedirectionEnabled);
      if (logRedirectionEnabled) {
        driver.setConsole(console);
      }
      // In Hive server mode, we are not able to retry in the FetchTask
      // case, when calling fetch queries since execute() has returned.
      // For now, we disable the test attempts.
      driver.setTryCount(Integer.MAX_VALUE);

      String subStatement = new VariableSubstitution().substitute(sqlOperationConf, statement);
      response = driver.compileAndRespond(subStatement);
      if (0 != response.getResponseCode()) {
        throw new HiveSQLException("Error while compiling statement: "
            + response.getErrorMessage(), response.getSQLState(), response.getResponseCode());
      }

      mResultSchema = driver.getSchema();

      // hasResultSet should be true only if the query has a FetchTask
      // "explain" is an exception for now
      if(driver.getPlan().getFetchTask() != null) {
        //Schema has to be set
        if (mResultSchema == null || !mResultSchema.isSetFieldSchemas()) {
          throw new HiveSQLException("Error compiling query: Schema and FieldSchema " +
              "should be set when query plan has a FetchTask");
        }
        resultSchema = new TableSchema(mResultSchema);
        setHasResultSet(true);
      } else {
        setHasResultSet(false);
      }
      // Set hasResultSet true if the plan has ExplainTask
      // TODO explain should use a FetchTask for reading
      for (Task<? extends Serializable> task: driver.getPlan().getRootTasks()) {
        if (task.getClass() == ExplainTask.class) {
          resultSchema = new TableSchema(mResultSchema);
          setHasResultSet(true);
          break;
        }
      }
    } catch (HiveSQLException e) {
      setState(OperationState.ERROR);
      throw e;
    } catch (Exception e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException("Error running query: " + e.toString(), e);
    }
  }

  private void runInternal(HiveConf sqlOperationConf) throws HiveSQLException {
    try {
      // In Hive server mode, we are not able to retry in the FetchTask
      // case, when calling fetch queries since execute() has returned.
      // For now, we disable the test attempts.
      driver.setTryCount(Integer.MAX_VALUE);
      response = driver.run();
      if (0 != response.getResponseCode()) {
        throw new HiveSQLException("Error while processing statement: "
            + response.getErrorMessage(), response.getSQLState(), response.getResponseCode());
      }
    } catch (HiveSQLException e) {
      // If the operation was cancelled by another thread,
      // Driver#run will return a non-zero response code.
      // We will simply return if the operation state is CANCELED,
      // otherwise throw an exception
      if (getStatus().getState() == OperationState.CANCELED) {
        return;
      }
      else {
        setState(OperationState.ERROR);
        throw e;
      }
    } catch (Exception e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException("Error running query: " + e.toString(), e);
    } finally {
      markOperationCompletedTime();
    }
    setState(OperationState.FINISHED);
  }
  @Override
  public void run() throws HiveSQLException {
    setState(OperationState.PENDING);
    markOperationStartTime();
    final HiveConf opConfig = getConfigForOperation();
    prepare(opConfig);
    if (!shouldRunAsync()) {
      runInternal(opConfig);
    } else {
      final SessionState parentSessionState = SessionState.get();
      // current Hive object needs to be set in aysnc thread in case of remote metastore.
      // The metastore client in Hive is associated with right user
      final Hive sessionHive = getCurrentHive();
      // current UGI will get used by metastore when metsatore is in embedded mode
      // so this needs to get passed to the new async thread
      final UserGroupInformation currentUGI = getCurrentUGI(opConfig);

      // Runnable impl to call runInternal asynchronously,
      // from a different thread
      Runnable backgroundOperation = new Runnable() {

        @Override
        public void run() {
          PrivilegedExceptionAction<Object> doAsAction = new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws HiveSQLException {

              // Storing the current Hive object necessary when doAs is enabled
              // User information is part of the metastore client member in Hive
              Hive.set(sessionHive);
              SessionState.setCurrentSessionState(parentSessionState);
              try {
                runInternal(opConfig);
              } catch (HiveSQLException e) {
                setOperationException(e);
                LOG.error("Error running hive query: ", e);
              }
              return null;
            }
          };
          try {
            ShimLoader.getHadoopShims().doAs(currentUGI, doAsAction);
          } catch (Exception e) {
            setOperationException(new HiveSQLException(e));
            LOG.error("Error running hive query as user : " + currentUGI.getShortUserName(), e);
          }
        }
      };
      try {
        // This submit blocks if no background threads are available to run this operation
        Future<?> backgroundHandle =
            getParentSession().getSessionManager().submitBackgroundOperation(backgroundOperation);
        setBackgroundHandle(backgroundHandle);
      } catch (RejectedExecutionException rejected) {
        setState(OperationState.ERROR);
        throw new HiveSQLException("The background threadpool cannot accept" +
            " new task for execution, please retry the operation", rejected);
      }
    }
  }

  private UserGroupInformation getCurrentUGI(HiveConf opConfig) throws HiveSQLException {
    try {
      return ShimLoader.getHadoopShims().getUGIForConf(opConfig);
    } catch (Exception e) {
      throw new HiveSQLException("Unable to get current user", e);
    }
  }

  private Hive getCurrentHive() throws HiveSQLException {
    try {
      return Hive.get();
    } catch (HiveException e) {
      throw new HiveSQLException("Failed to get current Hive object", e);
    }
  }

  private void cleanup(OperationState state) throws HiveSQLException {
    setState(state);
    if (shouldRunAsync()) {
      Future<?> backgroundHandle = getBackgroundHandle();
      if (backgroundHandle != null) {
        backgroundHandle.cancel(true);
      }
    }
    if (driver != null) {
      driver.close();
      driver.destroy();
    }
    driver = null;

    SessionState ss = SessionState.get();
    if (ss.getTmpOutputFile() != null) {
      ss.getTmpOutputFile().delete();
    }

    closeLogStreams();
  }

  @Override
  public void cancel() throws HiveSQLException {
    markOperationCompletedTime();
    cleanup(OperationState.CANCELED);
  }

  @Override
  public void close() throws HiveSQLException {
    cleanup(OperationState.CLOSED);
  }

  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    assertState(OperationState.FINISHED);
    if (resultSchema == null) {
      resultSchema = new TableSchema(driver.getSchema());
    }
    return resultSchema;
  }

  private transient final List<Object> convey = new ArrayList<Object>();

  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
    validateDefaultFetchOrientation(orientation);
    assertState(OperationState.FINISHED);

    RowSet rowSet = RowSetFactory.create(resultSchema, getProtocolVersion());

    try {
      /* if client is requesting fetch-from-start and its not the first time reading from this operation
       * then reset the fetch position to beginning
       */
      if (orientation.equals(FetchOrientation.FETCH_FIRST) && fetchStarted) {
        driver.resetFetch();
      }
      fetchStarted = true;
      driver.setMaxRows((int) maxRows);
      if (driver.getResults(convey)) {
        return decode(convey, rowSet);
      }
      return rowSet;
    } catch (IOException e) {
      throw new HiveSQLException(e);
    } catch (CommandNeedRetryException e) {
      throw new HiveSQLException(e);
    } catch (Exception e) {
      throw new HiveSQLException(e);
    } finally {
      convey.clear();
    }
  }

  private RowSet decode(List<Object> rows, RowSet rowSet) throws Exception {
    if (driver.isFetchingTable()) {
      return prepareFromRow(rows, rowSet);
    }
    return decodeFromString(rows, rowSet);
  }

  // already encoded to thrift-able object in ThriftFormatter
  private RowSet prepareFromRow(List<Object> rows, RowSet rowSet) throws Exception {
    for (Object row : rows) {
      rowSet.addRow((Object[]) row);
    }
    return rowSet;
  }

  private RowSet decodeFromString(List<Object> rows, RowSet rowSet)
      throws SQLException, SerDeException {
    getSerDe();
    StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();
    List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();

    Object[] deserializedFields = new Object[fieldRefs.size()];
    Object rowObj;
    ObjectInspector fieldOI;

    int protocol = getProtocolVersion().getValue();
    for (Object rowString : rows) {
      try {
        rowObj = serde.deserialize(new BytesWritable(((String)rowString).getBytes("UTF-8")));
      } catch (UnsupportedEncodingException e) {
        throw new SerDeException(e);
      }
      for (int i = 0; i < fieldRefs.size(); i++) {
        StructField fieldRef = fieldRefs.get(i);
        fieldOI = fieldRef.getFieldObjectInspector();
        Object fieldData = soi.getStructFieldData(rowObj, fieldRef);
        deserializedFields[i] = SerDeUtils.toThriftPayload(fieldData, fieldOI, protocol);
      }
      rowSet.addRow(deserializedFields);
    }
    return rowSet;
  }

  private SerDe getSerDe() throws SQLException {
    if (serde != null) {
      return serde;
    }
    try {
      List<FieldSchema> fieldSchemas = mResultSchema.getFieldSchemas();
      StringBuilder namesSb = new StringBuilder();
      StringBuilder typesSb = new StringBuilder();

      if (fieldSchemas != null && !fieldSchemas.isEmpty()) {
        for (int pos = 0; pos < fieldSchemas.size(); pos++) {
          if (pos != 0) {
            namesSb.append(",");
            typesSb.append(",");
          }
          namesSb.append(fieldSchemas.get(pos).getName());
          typesSb.append(fieldSchemas.get(pos).getType());
        }
      }
      String names = namesSb.toString();
      String types = typesSb.toString();

      serde = new LazySimpleSerDe();
      Properties props = new Properties();
      if (names.length() > 0) {
        LOG.debug("Column names: " + names);
        props.setProperty(serdeConstants.LIST_COLUMNS, names);
      }
      if (types.length() > 0) {
        LOG.debug("Column types: " + types);
        props.setProperty(serdeConstants.LIST_COLUMN_TYPES, types);
      }
      serde.initialize(new HiveConf(), props);

    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Could not create ResultSet: " + ex.getMessage(), ex);
    }
    return serde;
  }

  public String getQueryPlan() throws HiveSQLException {
    try {
      driver = new Driver(getParentSession().getHiveConf());
      int errorCode = driver.compile(statement);
      if (errorCode != 0) {
        throw new HiveSQLException("Error getting query plan: " + errorCode);
      } else {
        return driver.getPlan().toThriftJSONString();
      }
    } catch (IOException e) {
      throw new HiveSQLException(e);
    }
  }

  @Override
  public String getTaskStatus() throws HiveSQLException {
    if (driver != null) {
      List<TaskStatus> statuses = driver.getTaskStatuses();
      if (statuses != null) {
        ByteArrayOutputStream out = null;
        try {
          ObjectMapper mapper = new ObjectMapper();
          out = new ByteArrayOutputStream();
          mapper.writeValue(out, statuses);
          return out.toString("UTF-8");
        } catch (JsonGenerationException e) {
          throw new HiveSQLException(e);
        } catch (JsonMappingException e) {
          throw new HiveSQLException(e);
        } catch (IOException e) {
          throw new HiveSQLException(e);
        } finally {
          if (out != null) {
            try {
              out.close();
            } catch (IOException e) {
              throw new HiveSQLException(e);
            }
          }
        }
      }
    }

    // Driver not initialized
    return null;
  }

  /**
   * If there are query specific settings to overlay, then create a copy of config
   * There are two cases we need to clone the session config that's being passed to hive driver
   * 1. Async query -
   *    If the client changes a config setting, that shouldn't reflect in the execution already underway
   * 2. confOverlay -
   *    The query specific settings should only be applied to the query config and not session
   * @return new configuration
   * @throws HiveSQLException
   */
  private HiveConf getConfigForOperation() throws HiveSQLException {
    HiveConf sqlOperationConf = getParentSession().getHiveConf();
    if (!getConfOverlay().isEmpty() || shouldRunAsync()) {
      // clone the partent session config for this query
      sqlOperationConf = new HiveConf(sqlOperationConf);

      // apply overlay query specific settings, if any
      for (Map.Entry<String, String> confEntry : getConfOverlay().entrySet()) {
        try {
          sqlOperationConf.verifyAndSet(confEntry.getKey(), confEntry.getValue());
        } catch (IllegalArgumentException e) {
          // Ignore the conf setting if failed to set
          LOG.info("Ignoring the conf setting:" + confEntry);
        }
      }
    }
    return sqlOperationConf;
  }
}
