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

package org.apache.hive.service.cli;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.TaskStatus;
import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hadoop.hive.ql.plan.api.Stage;
import org.apache.hadoop.hive.ql.plan.api.Task;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.TException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


/**
 * CLIServiceTest.
 *
 */
public abstract class CLIServiceTest {

  protected static CLIServiceClient client;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void createSessionTest() throws Exception {
    SessionHandle sessionHandle = client
        .openSession("tom", "password", Collections.<String, String>emptyMap());
    assertNotNull(sessionHandle);
    client.closeSession(sessionHandle);

    sessionHandle = client.openSession("tom", "password");
    assertNotNull(sessionHandle);
    client.closeSession(sessionHandle);
  }

  @Test
  public void getFunctionsTest() throws Exception {
    SessionHandle sessionHandle = client.openSession("tom", "password", new HashMap<String, String>());
    assertNotNull(sessionHandle);
    OperationHandle opHandle = client.getFunctions(sessionHandle, null, null, "*");
    TableSchema schema = client.getResultSetMetadata(opHandle);

    ColumnDescriptor columnDesc = schema.getColumnDescriptorAt(0);
    assertEquals("FUNCTION_CAT", columnDesc.getName());
    assertEquals(Type.STRING_TYPE, columnDesc.getType());

    columnDesc = schema.getColumnDescriptorAt(1);
    assertEquals("FUNCTION_SCHEM", columnDesc.getName());
    assertEquals(Type.STRING_TYPE, columnDesc.getType());

    columnDesc = schema.getColumnDescriptorAt(2);
    assertEquals("FUNCTION_NAME", columnDesc.getName());
    assertEquals(Type.STRING_TYPE, columnDesc.getType());

    columnDesc = schema.getColumnDescriptorAt(3);
    assertEquals("REMARKS", columnDesc.getName());
    assertEquals(Type.STRING_TYPE, columnDesc.getType());

    columnDesc = schema.getColumnDescriptorAt(4);
    assertEquals("FUNCTION_TYPE", columnDesc.getName());
    assertEquals(Type.INT_TYPE, columnDesc.getType());

    columnDesc = schema.getColumnDescriptorAt(5);
    assertEquals("SPECIFIC_NAME", columnDesc.getName());
    assertEquals(Type.STRING_TYPE, columnDesc.getType());

    client.closeOperation(opHandle);
    client.closeSession(sessionHandle);
  }

  @Test
  public void getInfoTest() throws Exception {
    SessionHandle sessionHandle = client.openSession("tom", "password", new HashMap<String, String>());
    assertNotNull(sessionHandle);

    GetInfoValue value = client.getInfo(sessionHandle, GetInfoType.CLI_DBMS_NAME);
    System.out.println(value.getStringValue());

    value = client.getInfo(sessionHandle, GetInfoType.CLI_SERVER_NAME);
    System.out.println(value.getStringValue());

    value = client.getInfo(sessionHandle, GetInfoType.CLI_DBMS_VER);
    System.out.println(value.getStringValue());

    client.closeSession(sessionHandle);
  }

  @Test
  public void testExecuteStatement() throws Exception {
    HashMap<String, String> confOverlay = new HashMap<String, String>();
    SessionHandle sessionHandle = client.openSession("tom", "password",
        new HashMap<String, String>());
    assertNotNull(sessionHandle);

    // Change lock manager, otherwise unit-test doesn't go through
    String setLockMgr = "SET hive.lock.manager=" +
        "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager";
    confOverlay.put("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    client.executeStatement(sessionHandle, setLockMgr, confOverlay);

    String createTable = "CREATE TABLE TEST_EXEC(ID STRING)";
    client.executeStatement(sessionHandle, createTable, confOverlay);

    // blocking execute
    String select = "SELECT ID FROM TEST_EXEC";
    OperationHandle ophandle = client.executeStatement(sessionHandle, select, confOverlay);

    // expect query to be completed now
    assertEquals("Query should be finished",
        OperationState.FINISHED, client.getOperationStatus(ophandle).getState());
    client.closeSession(sessionHandle);
  }

  @Test
  public void testExecuteStatementAsync() throws Exception {
    HashMap<String, String> confOverlay = new HashMap<String, String>();
    SessionHandle sessionHandle = client.openSession("tom", "password",
        new HashMap<String, String>());
    // Timeout for the poll in case of asynchronous execute
    long pollTimeout = System.currentTimeMillis() + 100000;
    assertNotNull(sessionHandle);
    OperationState state = null;
    OperationHandle ophandle;

    // Change lock manager, otherwise unit-test doesn't go through
    confOverlay.put("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    confOverlay.put("test.conf.setting", "foobar");
    String createTable = "CREATE TABLE TEST_EXEC_ASYNC(ID STRING)";
    client.executeStatementAsync(sessionHandle, createTable, confOverlay);

    // Test async execution response when query is malformed
    String wrongQuery = "SELECT NAME FROM TEST_EXEC";
    ophandle = client.executeStatementAsync(sessionHandle, wrongQuery, confOverlay);

    int count = 0;
    while (true) {
      // Break if polling times out
      if (System.currentTimeMillis() > pollTimeout) {
          System.out.println("Polling timed out");
          break;
      }
      state = client.getOperationStatus(ophandle).getState();
      System.out.println("Polling: " + ophandle + " count=" + (++count)
          + " state=" + state);

      if (OperationState.CANCELED == state || state == OperationState.CLOSED
          || state == OperationState.FINISHED || state == OperationState.ERROR) {
        break;
      }
      Thread.sleep(1000);
    }

    assertEquals("Query should return an error state",
        OperationState.ERROR, client.getOperationStatus(ophandle).getState());

    // Test async execution when query is well formed
    String select = "SELECT ID FROM TEST_EXEC_ASYNC";
    ophandle =
        client.executeStatementAsync(sessionHandle, select, confOverlay);

    count = 0;
    boolean timedout = false;
    while (true) {
      // Break if polling times out
      if (System.currentTimeMillis() > pollTimeout) {
          System.out.println("Polling timed out");
          timedout = true;
          break;
      }
      state = client.getOperationStatus(ophandle).getState();
      System.out.println("Polling: " + ophandle + " count=" + (++count)
          + " state=" + state);

      if (OperationState.CANCELED == state || state == OperationState.CLOSED
          || state == OperationState.FINISHED || state == OperationState.ERROR) {
        break;
      }
      Thread.sleep(1000);
    }

    if (!timedout) {
      assertEquals("Query should be finished",
          OperationState.FINISHED, client.getOperationStatus(ophandle).getState());
    }

    // Cancellation test
    ophandle = client.executeStatementAsync(sessionHandle, select, confOverlay);
    System.out.println("cancelling " + ophandle);
    client.cancelOperation(ophandle);
    state = client.getOperationStatus(ophandle).getState();
    System.out.println(ophandle + " after cancelling, state= " + state);
    assertEquals("Query should be cancelled", OperationState.CANCELED, state);
    client.closeSession(sessionHandle);
  }

  @Test
  public void testSessionStateChange() throws Exception {
    HiveConf conf = new HiveConf(CLIServiceTest.class);
    SessionState before = SessionState.start(conf);
    SessionHandle sessionHandle = client.openSession("tom", "password",
      new HashMap<String, String>());
    Map<String, String> confOverlay = new HashMap<String, String>();
    client.executeStatement(sessionHandle, "DROP TABLE IF EXISTS SESSION_TEST", confOverlay);
    client.executeStatement(sessionHandle, "CREATE TABLE SESSION_TEST(ID STRING)", confOverlay);
    SessionState after = SessionState.get();
    assertTrue(before == after);
    client.closeSession(sessionHandle);
  }
}
