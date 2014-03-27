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
package org.apache.hadoop.hive.ql.session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test SessionState
 */
@RunWith(value = Parameterized.class)
public class TestSessionState {

  private final boolean prewarm;

  public TestSessionState(Boolean mode) {
    this.prewarm = mode.booleanValue();
  }

  @Parameters
  public static Collection<Boolean[]> data() {
    return Arrays.asList(new Boolean[][] { {false}, {true}});
  }

  @Before
  public void setup() {
    HiveConf conf = new HiveConf();
    if (prewarm) {
      HiveConf.setBoolVar(conf, ConfVars.HIVE_PREWARM_ENABLED, true);
      HiveConf.setIntVar(conf, ConfVars.HIVE_PREWARM_NUM_CONTAINERS, 1);
    }
    SessionState.start(conf);
  }

  /**
   * test set and get db
   */
  @Test
  public void testgetDbName() throws Exception {
    //check that we start with default db
    assertEquals(MetaStoreUtils.DEFAULT_DATABASE_NAME,
        SessionState.get().getCurrentDatabase());
    final String newdb = "DB_2";

    //set new db and verify get
    SessionState.get().setCurrentDatabase(newdb);
    assertEquals(newdb,
        SessionState.get().getCurrentDatabase());

    //verify that a new sessionstate has default db
    SessionState.start(new HiveConf());
    assertEquals(MetaStoreUtils.DEFAULT_DATABASE_NAME,
        SessionState.get().getCurrentDatabase());

  }

  @Test
  public void testClose() throws Exception {
    SessionState ss = SessionState.get();
    assertNull(ss.getTezSession());
    ss.close();
    assertNull(ss.getTezSession());
  }

  class RegisterJarRunnable implements Runnable {
    String jar;
    ClassLoader loader;
    SessionState ss;

    public RegisterJarRunnable(String jar, SessionState ss) {
      this.jar = jar;
      this.ss = ss;
    }

    public void run() {
      SessionState.start(ss);
      SessionState.registerJar(jar);
      loader = Thread.currentThread().getContextClassLoader();
    }
  }

  @Test
  public void testClassLoaderEquality() throws Exception {
    HiveConf conf = new HiveConf();
    final SessionState ss1 = new SessionState(conf);
    RegisterJarRunnable otherThread = new RegisterJarRunnable("./build/contrib/test/test-udfs.jar", ss1);
    Thread th1 = new Thread(otherThread);
    th1.start();
    th1.join();

    // set state in current thread
    SessionState.start(ss1);
    SessionState ss2 = SessionState.get();
    ClassLoader loader2 = ss2.conf.getClassLoader();

    System.out.println("Loader1:(Set in other thread) " + otherThread.loader);
    System.out.println("Loader2:(Set in SessionState.conf) " + loader2);
    System.out.println("Loader3:(CurrentThread.getContextClassLoader()) " + Thread.currentThread().getContextClassLoader());
    assertEquals("Other thread loader and session state loader", otherThread.loader, loader2);
    assertEquals("Other thread loader and current thread loader", otherThread.loader, Thread.currentThread().getContextClassLoader());
  }

  @Test
  public void testRegisterJarForDifferentThreads() {
    // Create a session state
    SessionState ss1 = new SessionState(new HiveConf());
    // Set the session state in current thread
    SessionState.start(ss1);
    // Add jar
    SessionState.registerJar("test__1__.jar");

    // Create another session state
    SessionState ss2 = new SessionState(new HiveConf());
    // Now set this one in the current thread
    SessionState.start(ss2);
    // Add another jar
    SessionState.registerJar("test__2__.jar");

    // Check if test1.jar is still present in the new session state
    ClassLoader ss2Loader = ss2.conf.getClassLoader();
    if (ss2Loader instanceof URLClassLoader) {
      URLClassLoader classLoader = (URLClassLoader) ss2Loader;
      List<URL> ss2Paths = new ArrayList<URL>();
      for (URL path : classLoader.getURLs()) {
        ss2Paths.add(path);
        if (path.toString().contains("test__1__.jar")) {
          System.out.println("SS2 CP URL: " + path);
          fail("Jar from old session should not be present here!");
        }
      }
    }
  }
}
