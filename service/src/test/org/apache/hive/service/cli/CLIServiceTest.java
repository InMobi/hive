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
  public void testGetQueryPlan() throws Exception {
    HashMap<String, String> confOverlay = new HashMap<String, String>();
    SessionHandle sessionHandle = client.openSession("tom", "password",
        new HashMap<String, String>());
    assertNotNull(sessionHandle);

    client.executeStatement(sessionHandle,
        "CREATE TABLE TEST_QUERY_PLAN_TAB(ID STRING)", confOverlay);

    String testquery = "SELECT ID FROM TEST_QUERY_PLAN_TAB";
    String queryPlan = client.getQueryPlan(sessionHandle, testquery, confOverlay);
    System.out.println("%%QUERY_PLAN:" + queryPlan);

    // Read the object back from JSON
    TMemoryBuffer tmb = new TMemoryBuffer(queryPlan.length());
    byte[] buf = queryPlan.getBytes("UTF-8");
    tmb.write(buf, 0, buf.length);
    TJSONProtocol oprot = new TJSONProtocol(tmb);
    Query plan = new Query();
    plan.read(oprot);
    
    assertNotNull(plan);
    List<Stage> stgList = plan.getStageList();
    assertNotNull(stgList);
    
    Driver expDriver = null;

    try {
      expDriver = new Driver(new HiveConf(this.getClass()));
      expDriver.compile(testquery);
      org.apache.hadoop.hive.ql.plan.api.Query expPlan = expDriver.getQueryPlan();
      
      assertEquals(expPlan.getStageList().size(), stgList.size());

      for (int i = 0; i < stgList.size(); i++) {
        Stage stg = stgList.get(i);
        Stage expStg = expPlan.getStageList().get(i);
        assertEquals(expStg.getStageType(), stg.getStageType());

        List<Task> stgTaskList = stg.getTaskList();
        assertNotNull(stgTaskList);
        List<Task> expStgTaskList = expStg.getTaskList();

        assertEquals(expStg.getTaskListSize(), stg.getTaskListSize());

        for (int j = 0; j < stgTaskList.size(); j++) {
          Task stgTask = stgTaskList.get(j);
          Task expTask = expStgTaskList.get(j);
          assertEquals(expTask.getTaskType(), stgTask.getTaskType());
        }
      }
    } finally {
      if (expDriver != null) {
        expDriver.close();
      }
    }
    
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
    client.executeStatement(sessionHandle, setLockMgr, confOverlay);

    String createTable = "CREATE TABLE TEST_EXEC_ASYNC(ID STRING)";
    client.executeStatement(sessionHandle, createTable, confOverlay);

    // nonblocking execute
    String select = "SELECT ID FROM TEST_EXEC_ASYNC";
    OperationHandle ophandle =
        client.executeStatementAsync(sessionHandle, select, confOverlay);

    OperationStatus status = null;
    int count = 0;
    while (true) {
      status = client.getOperationStatus(ophandle);
      OperationState state = status.getState();
      System.out.println("Polling: " + ophandle + " count=" + (++count) 
          + " state=" + state);
      
      String jsonTaskStatus = status.getTaskStatus();
      if (jsonTaskStatus != null) {
        System.out.println("JSON TASK STATUS:\n" + jsonTaskStatus);
        ObjectMapper mapper = new ObjectMapper();
        ByteArrayInputStream in = new ByteArrayInputStream(jsonTaskStatus.getBytes("UTF-8"));
        List<TaskStatus> taskStatuses = 
            mapper.readValue(in, new TypeReference<List<TaskStatus>>(){});
        assertNotNull(taskStatuses);
        System.out.println("Got statuses:" + taskStatuses.size());
        for (int i = 0 ; i < taskStatuses.size(); i++) {
          System.out.println(i + ": " + taskStatuses.get(i).toString());
        }
      } else {
        System.out.println("JSON STATUS NULL");
      }
      
      
      if (OperationState.CANCELED == state || state == OperationState.CLOSED
          || state == OperationState.FINISHED
          || state == OperationState.ERROR) {
        break;
      }
      Thread.sleep(1000);
    }

    assertEquals("Query should be finished",
        OperationState.FINISHED, client.getOperationStatus(ophandle).getState());

    // blocking execute
    ophandle = client.executeStatement(sessionHandle, select, confOverlay);
    // expect query to be completed now
    assertEquals("Query should be finished",
        OperationState.FINISHED, client.getOperationStatus(ophandle).getState());

    // cancellation test
    ophandle = client.executeStatementAsync(sessionHandle, select, confOverlay);
    System.out.println("cancelling " + ophandle);
    client.cancelOperation(ophandle);
    OperationState state = client.getOperationStatus(ophandle).getState();
    System.out.println(ophandle + " after cancelling, state= " + state);
    assertEquals("Query should be cancelled", OperationState.CANCELED, state);
  }
}
