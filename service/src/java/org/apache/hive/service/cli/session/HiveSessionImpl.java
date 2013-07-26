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

package org.apache.hive.service.cli.session;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.history.HiveHistory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.GetInfoValue;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.operation.ExecuteStatementOperation;
import org.apache.hive.service.cli.operation.GetCatalogsOperation;
import org.apache.hive.service.cli.operation.GetColumnsOperation;
import org.apache.hive.service.cli.operation.GetFunctionsOperation;
import org.apache.hive.service.cli.operation.GetSchemasOperation;
import org.apache.hive.service.cli.operation.GetTableTypesOperation;
import org.apache.hive.service.cli.operation.GetTypeInfoOperation;
import org.apache.hive.service.cli.operation.MetadataOperation;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.operation.SQLOperation;

/**
 * HiveSession
 *
 */
public class HiveSessionImpl implements HiveSession {

  private final SessionHandle sessionHandle = new SessionHandle();
  private String username;
  private final String password;
  private final Map<String, String> sessionConf = new HashMap<String, String>();
  private final HiveConf hiveConf = new HiveConf();
  private final SessionState sessionState;

  private static final String FETCH_WORK_SERDE_CLASS =
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
  private static final Log LOG = LogFactory.getLog(HiveSessionImpl.class);


  private SessionManager sessionManager;
  private OperationManager operationManager;
  private IMetaStoreClient metastoreClient = null;
  private final Set<OperationHandle> opHandleSet = new HashSet<OperationHandle>();

  public HiveSessionImpl(String username, String password, Map<String, String> sessionConf) {
    this.username = username;
    this.password = password;

    if (sessionConf != null) {
      for (Map.Entry<String, String> entry : sessionConf.entrySet()) {
        hiveConf.set(entry.getKey(), entry.getValue());
      }
    }
    // set an explicit session name to control the download directory name
    hiveConf.set(ConfVars.HIVESESSIONID.varname,
        sessionHandle.getHandleIdentifier().toString());
    sessionState = new SessionState(hiveConf);
  }

  public SessionManager getSessionManager() {
    return sessionManager;
  }

  public void setSessionManager(SessionManager sessionManager) {
    this.sessionManager = sessionManager;
  }

  private OperationManager getOperationManager() {
    return operationManager;
  }

  public void setOperationManager(OperationManager operationManager) {
    this.operationManager = operationManager;
  }

  protected synchronized void acquire() throws HiveSQLException {
    SessionState.start(sessionState);
  }

  protected synchronized void release() {
    assert sessionState != null;
    // no need to release sessionState...
  }

  public SessionHandle getSessionHandle() {
    return sessionHandle;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public HiveConf getHiveConf() {
    hiveConf.setVar(HiveConf.ConfVars.HIVEFETCHOUTPUTSERDE, FETCH_WORK_SERDE_CLASS);
    return hiveConf;
  }

  public IMetaStoreClient getMetaStoreClient() throws HiveSQLException {
    if (metastoreClient == null) {
      try {
        metastoreClient = new HiveMetaStoreClient(getHiveConf());
      } catch (MetaException e) {
        throw new HiveSQLException(e);
      }
    }
    return metastoreClient;
  }

  public GetInfoValue getInfo(GetInfoType getInfoType)
      throws HiveSQLException {
    acquire();
    try {
      switch (getInfoType) {
      case CLI_SERVER_NAME:
        return new GetInfoValue("Hive");
      case CLI_DBMS_NAME:
        return new GetInfoValue("Apache Hive");
      case CLI_DBMS_VER:
        return new GetInfoValue(HiveVersionInfo.getVersion());
      case CLI_MAX_COLUMN_NAME_LEN:
        return new GetInfoValue(128);
      case CLI_MAX_SCHEMA_NAME_LEN:
        return new GetInfoValue(128);
      case CLI_MAX_TABLE_NAME_LEN:
        return new GetInfoValue(128);
      case CLI_TXN_CAPABLE:
      default:
        throw new HiveSQLException("Unrecognized GetInfoType value: " + getInfoType.toString());
      }
    } finally {
      release();
    }
  }

  public OperationHandle executeStatement(String statement, Map<String, String> confOverlay,
      boolean runAsync)
      throws HiveSQLException {
    acquire();
    try {
      ExecuteStatementOperation operation = getOperationManager()
          .newExecuteStatementOperation(getSession(), statement, confOverlay);
      if (operation instanceof SQLOperation) {
        ((SQLOperation) operation).setRunAsync(runAsync);
        ((SQLOperation) operation).setSessionState(SessionState.get());
      }
      operation.run();
      OperationHandle opHandle = operation.getHandle();
      opHandleSet.add(opHandle);
      return opHandle;
    } finally {
      release();
    }
  }

  public OperationHandle getTypeInfo()
      throws HiveSQLException {
    acquire();
    try {
      GetTypeInfoOperation operation = getOperationManager().newGetTypeInfoOperation(getSession());
      operation.run();
      OperationHandle opHandle = operation.getHandle();
      opHandleSet.add(opHandle);
      return opHandle;
    } finally {
      release();
    }
  }

  public OperationHandle getCatalogs()
      throws HiveSQLException {
    acquire();
    try {
      GetCatalogsOperation operation = getOperationManager().newGetCatalogsOperation(getSession());
      operation.run();
      OperationHandle opHandle = operation.getHandle();
      opHandleSet.add(opHandle);
      return opHandle;
    } finally {
      release();
    }
  }

  public OperationHandle getSchemas(String catalogName, String schemaName)
      throws HiveSQLException {
      acquire();
    try {
      GetSchemasOperation operation =
          getOperationManager().newGetSchemasOperation(getSession(), catalogName, schemaName);
      operation.run();
      OperationHandle opHandle = operation.getHandle();
      opHandleSet.add(opHandle);
      return opHandle;
    } finally {
      release();
    }
  }

  public OperationHandle getTables(String catalogName, String schemaName, String tableName,
      List<String> tableTypes)
      throws HiveSQLException {
      acquire();
    try {
      MetadataOperation operation =
          getOperationManager().newGetTablesOperation(getSession(), catalogName, schemaName, tableName, tableTypes);
      operation.run();
      OperationHandle opHandle = operation.getHandle();
      opHandleSet.add(opHandle);
      return opHandle;
    } finally {
      release();
    }
  }

  public OperationHandle getTableTypes()
      throws HiveSQLException {
      acquire();
    try {
      GetTableTypesOperation operation = getOperationManager().newGetTableTypesOperation(getSession());
      operation.run();
      OperationHandle opHandle = operation.getHandle();
      opHandleSet.add(opHandle);
      return opHandle;
    } finally {
      release();
    }
  }

  public OperationHandle getColumns(String catalogName, String schemaName,
      String tableName, String columnName)  throws HiveSQLException {
    acquire();
    try {
    GetColumnsOperation operation = getOperationManager().newGetColumnsOperation(getSession(),
        catalogName, schemaName, tableName, columnName);
    operation.run();
    OperationHandle opHandle = operation.getHandle();
    opHandleSet.add(opHandle);
    return opHandle;
    } finally {
      release();
    }
  }

  public OperationHandle getFunctions(String catalogName, String schemaName, String functionName)
      throws HiveSQLException {
    acquire();
    try {
      GetFunctionsOperation operation = getOperationManager()
          .newGetFunctionsOperation(getSession(), catalogName, schemaName, functionName);
      operation.run();
      OperationHandle opHandle = operation.getHandle();
      opHandleSet.add(opHandle);
      return opHandle;
    } finally {
      release();
    }
  }

  public void close() throws HiveSQLException {
    try {
      acquire();
      /**
       *  For metadata operations like getTables(), getColumns() etc,
       * the session allocates a private metastore handler which should be
       * closed at the end of the session
       */
      if (metastoreClient != null) {
        metastoreClient.close();
      }
      // Iterate through the opHandles and close their operations
      for (OperationHandle opHandle : opHandleSet) {
        operationManager.closeOperation(opHandle);
      }
      opHandleSet.clear();
      HiveHistory hiveHist = sessionState.getHiveHistory();
      if (null != hiveHist) {
        hiveHist.closeStream();
      }
      sessionState.close();
      release();
    } catch (IOException ioe) {
      release();
      throw new HiveSQLException("Failure to close", ioe);
    }
  }

  public SessionState getSessionState() {
    return sessionState;
  }

  public String getUserName() {
    return username;
  }
  public void setUserName(String userName) {
    this.username = userName;
  }

  @Override
  public void cancelOperation(OperationHandle opHandle) throws HiveSQLException {
    acquire();
    try {
      sessionManager.getOperationManager().cancelOperation(opHandle);
    } finally {
      release();
    }
  }

  @Override
  public void closeOperation(OperationHandle opHandle) throws HiveSQLException {
    acquire();
    try {
      operationManager.closeOperation(opHandle);
      opHandleSet.remove(opHandle);
    } finally {
      release();
    }
  }

  @Override
  public TableSchema getResultSetMetadata(OperationHandle opHandle) throws HiveSQLException {
    acquire();
    try {
      return sessionManager.getOperationManager().getOperationResultSetSchema(opHandle);
    } finally {
      release();
    }
  }

  @Override
  public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation, long maxRows)
      throws HiveSQLException {
    acquire();
    try {
      return sessionManager.getOperationManager()
          .getOperationNextRowSet(opHandle, orientation, maxRows);
    } finally {
      release();
    }
  }

  @Override
  public RowSet fetchResults(OperationHandle opHandle) throws HiveSQLException {
    acquire();
    try {
      return sessionManager.getOperationManager().getOperationNextRowSet(opHandle);
    } finally {
      release();
    }
  }

  protected HiveSession getSession() {
    return this;
  }

  @Override
  public String getQueryPlan(String statement, Map<String, String> params)
      throws HiveSQLException {
    ExecuteStatementOperation operation = getOperationManager()
        .newExecuteStatementOperation(getSession(), statement, params);

    if (operation instanceof SQLOperation) {
      SQLOperation sqlOperation = (SQLOperation) operation;
      return sqlOperation.getQueryPlan();
    } else {
      throw new HiveSQLException("Not an SQL statement: " + statement);
    }
  }
}
