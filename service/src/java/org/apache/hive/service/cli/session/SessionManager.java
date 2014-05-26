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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.auth.TSetIpAddressProcessor;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.thrift.TProtocolVersion;

/**
 * SessionManager.
 *
 */
public class SessionManager extends CompositeService {

  private static final Log LOG = LogFactory.getLog(CompositeService.class);
  private HiveConf hiveConf;
  private final Map<SessionHandle, HiveSession> handleToSession =
      new ConcurrentHashMap<SessionHandle, HiveSession>();
  private final OperationManager operationManager = new OperationManager();
  private ThreadPoolExecutor backgroundOperationPool;
  private ScheduledExecutorService logPurgerService;
  private File queryLogDir;
  private boolean isLogRedirectionEnabled;
  private String sessionImplWithUGIclassName;
  private String sessionImplclassName;

  public SessionManager() {
    super("SessionManager");
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    try {
      applyAuthorizationConfigPolicy(hiveConf);
    } catch (HiveException e) {
      throw new RuntimeException("Error applying authorization policy on hive configuration", e);
    }

    this.hiveConf = hiveConf;
    int backgroundPoolSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS);
    LOG.info("HiveServer2: Async execution thread pool size: " + backgroundPoolSize);
    int backgroundPoolQueueSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_WAIT_QUEUE_SIZE);
    LOG.info("HiveServer2: Async execution wait queue size: " + backgroundPoolQueueSize);
    int keepAliveTime = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_KEEPALIVE_TIME);
    LOG.info("HiveServer2: Async execution thread keepalive time: " + keepAliveTime);
    // Create a thread pool with #backgroundPoolSize threads
    // Threads terminate when they are idle for more than the keepAliveTime
    // An bounded blocking queue is used to queue incoming operations, if #operations > backgroundPoolSize
    backgroundOperationPool = new ThreadPoolExecutor(backgroundPoolSize, backgroundPoolSize,
        keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(backgroundPoolQueueSize));
    backgroundOperationPool.allowCoreThreadTimeOut(true);

    this.sessionImplclassName = hiveConf.getVar(ConfVars.HIVE_SESSION_IMPL_CLASSNAME);
    this.sessionImplWithUGIclassName = hiveConf.getVar(ConfVars.HIVE_SESSION_IMPL_WITH_UGI_CLASSNAME);
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_LOG_REDIRECTION_ENABLED)) {
      queryLogDir = new File(hiveConf.getVar(ConfVars.HIVE_SERVER2_LOG_DIRECTORY));
      isLogRedirectionEnabled = true;
      if (queryLogDir.exists() && !queryLogDir.isDirectory()) {
        LOG.warn("Query logs - not a directory! " + queryLogDir.getAbsolutePath());
        isLogRedirectionEnabled = false;
      }

      if (!queryLogDir.exists()) {
        if (!queryLogDir.mkdir()) {
          LOG.warn("Query logs - unable to create query log directory - " + queryLogDir.getAbsolutePath());
          isLogRedirectionEnabled = false;
        }
      }

      if (isLogRedirectionEnabled) {
        logPurgerService = Executors.newSingleThreadScheduledExecutor();
        long purgeDelay = hiveConf.getLongVar(ConfVars.HIVE_SERVER2_LOG_PURGE_DELAY);
        QueryLogPurger purger = new QueryLogPurger(queryLogDir, purgeDelay);
        logPurgerService.scheduleWithFixedDelay(purger, 60, 60, TimeUnit.SECONDS);
        LOG.info("Started log purger service");
      }
    }

    addService(operationManager);
    super.init(hiveConf);
  }

  private void applyAuthorizationConfigPolicy(HiveConf newHiveConf) throws HiveException {
    // authorization setup using SessionState should be revisited eventually, as
    // authorization and authentication are not session specific settings
    SessionState ss = SessionState.start(newHiveConf);
    ss.applyAuthorizationPolicy();
  }

  @Override
  public synchronized void start() {
    super.start();
  }

  @Override
  public synchronized void stop() {
    super.stop();
    if (backgroundOperationPool != null) {
      backgroundOperationPool.shutdown();
      int timeout = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT);
      try {
        backgroundOperationPool.awaitTermination(timeout, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT = " + timeout +
            " seconds has been exceeded. RUNNING background operations will be shut down", e);
      }
    }

    if (logPurgerService != null) {
      logPurgerService.shutdownNow();
    }
  }

  public SessionHandle openSession(TProtocolVersion protocol, String username, String password,
      Map<String, String> sessionConf) throws HiveSQLException {
    return openSession(protocol, username, password, sessionConf, false, null);
  }


  public SessionHandle openSession(TProtocolVersion protocol, String username, String password,
      Map<String, String> sessionConf, boolean withImpersonation, String delegationToken)
          throws HiveSQLException {
    HiveSession session = createSession(null, protocol, username, password, sessionConf,
      withImpersonation, delegationToken);
    return session.getSessionHandle();
  }

  private HiveSession createSession(SessionHandle sessionHandle,
                                    TProtocolVersion protocol,
                                    String username,
                                    String password,
                                    Map<String, String> sessionConf,
                                    boolean withImpersonation,
                                    String delegationToken) throws HiveSQLException {
    HiveSession session;
    if (withImpersonation) {
      HiveSessionImplwithUGI hiveSessionUgi;
      if (sessionImplWithUGIclassName == null) {
        hiveSessionUgi = new HiveSessionImplwithUGI(protocol, username, password,
          hiveConf, sessionConf, TSetIpAddressProcessor.getUserIpAddress(), delegationToken);
      } else {
        try {
          Class<?> clazz = Class.forName(sessionImplWithUGIclassName);
          Constructor<?> constructor = clazz.getConstructor(TProtocolVersion.class,
            String.class, String.class, HiveConf.class, Map.class, String.class, String.class);
          hiveSessionUgi = (HiveSessionImplwithUGI) constructor.newInstance(new Object[]
            {protocol, username, password, hiveConf, sessionConf,
              TSetIpAddressProcessor.getUserIpAddress(), delegationToken});
        } catch (Exception e) {
          throw new HiveSQLException("Cannot initilize session class:" + sessionImplWithUGIclassName);
        }
      }
      session = HiveSessionProxy.getProxy(hiveSessionUgi, hiveSessionUgi.getSessionUgi());
      hiveSessionUgi.setProxySession(session);
    } else {
      if (sessionImplclassName == null) {
        session = new HiveSessionImpl(protocol, username, password, hiveConf, sessionConf,
          TSetIpAddressProcessor.getUserIpAddress());
      } else {
        try {
          if (sessionHandle != null) {
            Class<?> clazz = Class.forName(sessionImplclassName);
            Constructor<?> constructor = clazz.getConstructor(SessionHandle.class, TProtocolVersion.class,
              String.class, String.class, HiveConf.class, Map.class, String.class);
            session = (HiveSession) constructor.newInstance(sessionHandle, protocol, username, password,
              hiveConf, sessionConf, TSetIpAddressProcessor.getUserIpAddress());
          } else {
            Class<?> clazz = Class.forName(sessionImplclassName);
            Constructor<?> constructor = clazz.getConstructor(TProtocolVersion.class,
              String.class, String.class, HiveConf.class, Map.class, String.class);
            session = (HiveSession) constructor.newInstance(protocol, username, password,
              hiveConf, sessionConf, TSetIpAddressProcessor.getUserIpAddress());
          }
        } catch (Exception e) {
          throw new HiveSQLException("Cannot initilize session class:" + sessionImplclassName, e);
        }
      }
    }

    session.setSessionManager(this);
    session.setOperationManager(operationManager);
    if (isLogRedirectionEnabled) {
      session.setupLogRedirection(queryLogDir);
    }
    session.setSessionManager(this);
    session.setOperationManager(operationManager);
    session.open();
    handleToSession.put(session.getSessionHandle(), session);

    try {
      executeSessionHooks(session);
    } catch (Exception e) {
      throw new HiveSQLException("Failed to execute session hooks", e);
    }

    return session;
  }

  public SessionHandle restoreSession(SessionHandle sessionHandle, TProtocolVersion protocol, String username, String password,
                                   Map<String, String> sessionConf, boolean withImpersonation, String delegationToken)
    throws HiveSQLException {
    HiveSession session = createSession(sessionHandle, protocol, username, password, sessionConf,
      withImpersonation, delegationToken);
    return session.getSessionHandle();
  }

  public void closeSession(SessionHandle sessionHandle) throws HiveSQLException {
    HiveSession session = handleToSession.remove(sessionHandle);
    if (session == null) {
      throw new HiveSQLException("Session does not exist!");
    }
    session.close();
  }

  public HiveSession getSession(SessionHandle sessionHandle) throws HiveSQLException {
    HiveSession session = handleToSession.get(sessionHandle);
    if (session == null) {
      throw new HiveSQLException("Invalid SessionHandle: " + sessionHandle);
    }
    return session;
  }

  public OperationManager getOperationManager() {
    return operationManager;
  }

  private static ThreadLocal<String> threadLocalIpAddress = new ThreadLocal<String>() {
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  public static void setIpAddress(String ipAddress) {
    threadLocalIpAddress.set(ipAddress);
  }

  public static void clearIpAddress() {
    threadLocalIpAddress.remove();
  }

  public static String getIpAddress() {
    return threadLocalIpAddress.get();
  }

  private static ThreadLocal<String> threadLocalUserName = new ThreadLocal<String>(){
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  public static void setUserName(String userName) {
    threadLocalUserName.set(userName);
  }

  public static void clearUserName() {
    threadLocalUserName.remove();
  }

  public static String getUserName() {
    return threadLocalUserName.get();
  }

  private static ThreadLocal<String> threadLocalProxyUserName = new ThreadLocal<String>(){
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  public static void setProxyUserName(String userName) {
    LOG.debug("setting proxy user name based on query param to: " + userName);
    threadLocalProxyUserName.set(userName);
  }

  public static String getProxyUserName() {
    return threadLocalProxyUserName.get();
  }

  public static void clearProxyUserName() {
    threadLocalProxyUserName.remove();
  }

  // execute session hooks
  private void executeSessionHooks(HiveSession session) throws Exception {
    List<HiveSessionHook> sessionHooks = HookUtils.getHooks(hiveConf,
        HiveConf.ConfVars.HIVE_SERVER2_SESSION_HOOK, HiveSessionHook.class);
    for (HiveSessionHook sessionHook : sessionHooks) {
      sessionHook.run(new HiveSessionHookContextImpl(session));
    }
  }

  public Future<?> submitBackgroundOperation(Runnable r) {
    return backgroundOperationPool.submit(r);
  }

  public static class QueryLogPurger implements Runnable {
    private final File queryLogDir;
    private final long purgeDelay;

    /**
     * Log purging runnable.
     * @param dir location of query logs
     * @param delay period after which a closed session's log will be deleted. If this is negative or zero, it means
     *              logs are eligible for purging immediately after the session is closed
     */
    public QueryLogPurger(File dir, long delay) {
      if (dir == null) {
        throw new NullPointerException("Query log directory cannot be null");
      }
      queryLogDir = dir;
      purgeDelay = delay;
    }

    private void deleteDirectory(File dir, long age, boolean sessionClosed) {
      LOG.info("Query log purger - begin delete logs " + dir.getPath()
        + ", inactive since " + age + ", session closed? " + (sessionClosed ? "yes" : "no"));
      try {
        FileUtils.forceDelete(dir);
        LOG.info("Query log purger - deleted logs " + dir.getPath());
      } catch (IOException e) {
        LOG.error("Error deleting session logs ", e);
      }
    }

    @Override
    public void run() {
      File[] sessions = queryLogDir.listFiles();
      if (sessions == null) {
        return;
      }

      for (File session : sessions) {
        if (session.isDirectory()) {
          long inactiveSince;
          boolean sessionClosed = false;
          File sessionClosedMarker = new File(session, HiveSession.SESSION_CLOSED_MARKER);

          if (sessionClosedMarker.exists()) {
            inactiveSince = System.currentTimeMillis() - sessionClosedMarker.lastModified();
            sessionClosed = true;
          } else {
            // Find last modified log file for this session, since closed marker is not present
            // Check for sessions with no operations created in a long time
            long lastModifiedTime = session.lastModified();
            if (System.currentTimeMillis() - lastModifiedTime >= purgeDelay) {
              Queue<File> queue = new LinkedList<File>();
              queue.add(session);

              while (queue.isEmpty()) {
                File f = queue.poll();
                if (lastModifiedTime < f.lastModified()) {
                  lastModifiedTime = f.lastModified();
                }
                if (f.isDirectory()) {
                  // Its an operation log dir
                  long operationLastLogTime = f.lastModified();
                  File[] logFiles = f.listFiles();
                  if (logFiles != null) {
                    for (File child : logFiles) {
                      if (operationLastLogTime < child.lastModified()) {
                        operationLastLogTime = child.lastModified();
                      }
                      queue.offer(child);
                    }
                  }
                  if (System.currentTimeMillis() - operationLastLogTime >= purgeDelay) {
                    // there hasn't been anything logged for this operation for a long time
                    // assume that this operation is done and delete its logs
                    deleteDirectory(f, System.currentTimeMillis() - operationLastLogTime, false);
                  }
                }
              }
            }
            inactiveSince = System.currentTimeMillis() - lastModifiedTime;
          }

          if (inactiveSince >= purgeDelay) {
            deleteDirectory(session, inactiveSince, sessionClosed);
          }
        }
      }
    }
  }
}

