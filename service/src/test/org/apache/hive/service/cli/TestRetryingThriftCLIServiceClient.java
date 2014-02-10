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

import com.sun.net.httpserver.Authenticator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.cli.thrift.*;
import org.apache.hive.service.server.HiveServer2;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Test CLI service with a retrying client. All tests should pass. This is to validate that calls
 * are transferred successfully.
 */
public class TestRetryingThriftCLIServiceClient extends TestEmbeddedThriftBinaryCLIService {
  protected static ThriftCLIService service;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    service = new EmbeddedThriftBinaryCLIService();
    client = RetryingThriftCLIServiceClient.newRetryingCLIServiceClient(new HiveConf(), service);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceTest#setUp()
   */
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceTest#tearDown()
   */
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  static class RetryingThriftCLIServiceClientTest extends RetryingThriftCLIServiceClient {
    int callCount = 0;
    static RetryingThriftCLIServiceClientTest handlerInst;

    protected RetryingThriftCLIServiceClientTest(HiveConf conf, TCLIService.Iface thriftClient) {
      super(conf, thriftClient);
    }

    public static CLIServiceClient newRetryingCLIServiceClient(HiveConf conf, TCLIService.Iface thriftClient) {
      handlerInst = new RetryingThriftCLIServiceClientTest(conf, thriftClient);
      InvocationHandler handler = handlerInst;

      ICLIService cliService =
        (ICLIService) Proxy.newProxyInstance(RetryingThriftCLIServiceClientTest.class.getClassLoader(),
          CLIServiceClient.class.getInterfaces(), handler);
      return new CLIServiceClientWrapper(cliService);
    }

    @Override
    protected InvocationResult invokeInternal(Method method, Object[] args) throws Throwable {
      System.out.println("## Calling: " + method.getName() + ", " + callCount + "/" + getRetryLimit());
      callCount++;
      return super.invokeInternal(method, args);
    }
  }
  @Test
  public void testRetryBehaviour() throws Exception {
    // Start hive server2
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, "localhost");
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, 15000);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION, HiveAuthFactory.AuthTypes.NOSASL.toString());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE, "binary");
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_RETRY_LIMIT, 3);
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS, 10);
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT, 1);

    final HiveServer2 server = new HiveServer2();
    server.init(hiveConf);
    Thread serverTh = new Thread(new Runnable() {
      @Override
      public void run() {
        server.start();
      }
    });
    serverTh.start();
    Thread.sleep(2500);
    System.out.println("## HiveServer started");

    // Create client
    TTransport transport = PlainSaslHelper.getPlainTransport("anonymous", "anonymous",
      new TSocket("localhost", 15000));
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    TCLIService.Client client = new TCLIService.Client(protocol);
    CLIServiceClient cliServiceClient =
      RetryingThriftCLIServiceClientTest.newRetryingCLIServiceClient(hiveConf, client);
    System.out.println("## Created client");

    // kill server
    server.stop();
    Thread.sleep(5000);

    // submit few queries
    try {
      Map<String, String> confOverlay = new HashMap<String, String>();
      SessionHandle session = cliServiceClient.openSession("anonymous", "anonymous");
      RetryingThriftCLIServiceClientTest.handlerInst.callCount = 0;
      OperationHandle operation =
        cliServiceClient.executeStatement(session, "CREATE TABLE RETRY_TEST(ID STRING)", confOverlay);
    } catch (HiveSQLException exc) {
      assertTrue(exc.getCause() instanceof TException);
      assertEquals(3, RetryingThriftCLIServiceClientTest.handlerInst.callCount);
    }

  }
}
