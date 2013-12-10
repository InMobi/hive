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
import java.util.List;
import java.util.Map;
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
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.operation.OperationManager;

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

  public SessionManager() {
    super("SessionManager");
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
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

      logPurgerService = Executors.newSingleThreadScheduledExecutor();
      long purgeDelay = hiveConf.getLongVar(ConfVars.HIVE_SERVER2_LOG_PURGE_DELAY);
      QueryLogPurger purger = new QueryLogPurger(queryLogDir, purgeDelay);
      logPurgerService.scheduleWithFixedDelay(purger, 60, 60, TimeUnit.SECONDS);
      LOG.info("Started log purger service");
    }

    addService(operationManager);
    super.init(hiveConf);
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

  public SessionHandle openSession(String username, String password, Map<String, String> sessionConf)
          throws HiveSQLException {
     return openSession(username, password, sessionConf, false, null);
  }

  public SessionHandle openSession(String username, String password, Map<String, String> sessionConf,
          boolean withImpersonation, String delegationToken) throws HiveSQLException {
    if (username == null) {
      username = threadLocalUserName.get();
    }
    HiveSession session;
    if (withImpersonation) {
      HiveSessionImplwithUGI hiveSessionUgi = new HiveSessionImplwithUGI(username, password,
        sessionConf, delegationToken);
      session = HiveSessionProxy.getProxy(hiveSessionUgi, hiveSessionUgi.getSessionUgi());
      hiveSessionUgi.setProxySession(session);
    } else {
      session = new HiveSessionImpl(username, password, sessionConf);
    }
    session.setSessionManager(this);
    session.setOperationManager(operationManager);
    if (isLogRedirectionEnabled) {
      session.setupLogRedirection(queryLogDir);
    }

    handleToSession.put(session.getSessionHandle(), session);

    try {
      executeSessionHooks(session);
    } catch (Exception e) {
      throw new HiveSQLException("Failed to execute session hooks", e);
    }
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

  private void clearIpAddress() {
    threadLocalIpAddress.remove();
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

  private void clearUserName() {
    threadLocalUserName.remove();
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

    @Override
    public void run() {
      long now = System.currentTimeMillis();
      for (File session : queryLogDir.listFiles()) {
        if (session.isDirectory()) {
          File sessionClosedMarker = new File(session, HiveSession.SESSION_CLOSED_MARKER);
          if (sessionClosedMarker.exists()) {
            long sessionClosedSince = now - sessionClosedMarker.lastModified();
            if (sessionClosedSince >= purgeDelay) {
              LOG.info("Query log purger - begin delete session " + session.getName()
                + " closed since " + sessionClosedSince);
              try {
                FileUtils.forceDelete(session);
                LOG.info("Query log purger - deleted logs of session " + session.getName());
              } catch (IOException e) {
                LOG.error("Error deleting session logs ", e);
              }

            }
          }
        }
      }
    }
  }
}
