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
package org.apache.hadoop.hive.metastore.txn;

import com.google.common.annotations.VisibleForTesting;
import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.dbcp.PoolingDataSource;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.StringUtils;

import javax.sql.DataSource;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A handler to answer transaction related calls that come into the metastore
 * server.
 *
 * Note on log messages:  Please include txnid:X and lockid info using
 * {@link org.apache.hadoop.hive.common.JavaUtils#txnIdToString(long)}
 * and {@link org.apache.hadoop.hive.common.JavaUtils#lockIdToString(long)} in all messages.
 * The txnid:X and lockid:Y matches how Thrift object toString() methods are generated,
 * so keeping the format consistent makes grep'ing the logs much easier.
 *
 * Note on HIVE_LOCKS.hl_last_heartbeat.
 * For locks that are part of transaction, we set this 0 (would rather set it to NULL but
 * Currently the DB schema has this NOT NULL) and only update/read heartbeat from corresponding
 * transaction in TXNS.
 *
 * In general there can be multiple metastores where this logic can execute, thus the DB is
 * used to ensure proper mutexing of operations.
 * Select ... For Update (or equivalent: either MsSql with(updlock) or actual Update stmt) is
 * used to properly sequence operations.  Most notably:
 * 1. various sequence IDs are generated with aid of this mutex
 * 2. ensuring that each (Hive) Transaction state is transitioned atomically.  Transaction state
 *  includes it's actual state (Open, Aborted) as well as it's lock list/component list.  Thus all
 *  per transaction ops, either start by update/delete of the relevant TXNS row or do S4U on that row.
 *  This allows almost all operations to run at READ_COMMITTED and minimizes DB deadlocks.
 * 3. checkLock() - this is mutexted entirely since we must ensure that while we check if some lock
 *  can be granted, no other (strictly speaking "earlier") lock can change state.
 *
 * The exception to his is Derby which doesn't support proper S4U.  Derby is always running embedded
 * (this is the only supported configuration for Derby)
 * in the same JVM as HiveMetaStoreHandler thus we use JVM wide lock to properly sequnce the operations.
 *
 * {@link #derbyLock}

 * If we ever decide to run remote Derby server, according to
 * https://db.apache.org/derby/docs/10.0/manuals/develop/develop78.html all transactions will be
 * seriazlied, so that would also work though has not been tested.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
abstract class TxnHandler implements TxnStore, TxnStore.MutexAPI {

  static final protected char INITIATED_STATE = 'i';
  static final protected char WORKING_STATE = 'w';
  static final protected char READY_FOR_CLEANING = 'r';
  static final char FAILED_STATE = 'f';
  static final char SUCCEEDED_STATE = 's';
  static final char ATTEMPTED_STATE = 'a';

  // Compactor types
  static final protected char MAJOR_TYPE = 'a';
  static final protected char MINOR_TYPE = 'i';

  // Transaction states
  static final protected char TXN_ABORTED = 'a';
  static final protected char TXN_OPEN = 'o';

  // Lock states
  static final protected char LOCK_ACQUIRED = 'a';
  static final protected char LOCK_WAITING = 'w';

  // Lock types
  static final protected char LOCK_EXCLUSIVE = 'e';
  static final protected char LOCK_SHARED = 'r';
  static final protected char LOCK_SEMI_SHARED = 'w';

  static final private int ALLOWED_REPEATED_DEADLOCKS = 10;
  static final private Logger LOG = LoggerFactory.getLogger(TxnHandler.class.getName());

  static private DataSource connPool;
  static private boolean doRetryOnConnPool = false;

  /**
   * Number of consecutive deadlocks we have seen
   */
  private int deadlockCnt;
  private long deadlockRetryInterval;
  protected HiveConf conf;
  protected DatabaseProduct dbProduct;

  // (End user) Transaction timeout, in milliseconds.
  private long timeout;

  private String identifierQuoteString; // quotes to use for quoting tables, where necessary
  private long retryInterval;
  private int retryLimit;
  private int retryNum;
  /**
   * Derby specific concurrency control
   */
  private static final ReentrantLock derbyLock = new ReentrantLock(true);
  /**
   * must be static since even in UT there may be > 1 instance of TxnHandler
   * (e.g. via Compactor services)
   */
  private final static ConcurrentHashMap<String, Semaphore> derbyKey2Lock = new ConcurrentHashMap<>();
  private static final String hostname = ServerUtils.hostname();

  // Private methods should never catch SQLException and then throw MetaException.  The public
  // methods depend on SQLException coming back so they can detect and handle deadlocks.  Private
  // methods should only throw MetaException when they explicitly know there's a logic error and
  // they want to throw past the public methods.
  //
  // All public methods that write to the database have to check for deadlocks when a SQLException
  // comes back and handle it if they see one.  This has to be done with the connection pooling
  // in mind.  To do this they should call checkRetryable() AFTER rolling back the db transaction,
  // and then they should catch RetryException and call themselves recursively. See commitTxn for an example.

  public TxnHandler() {
  }

  /**
   * This is logically part of c'tor and must be called prior to any other method.
   * Not physically part of c'tor due to use of relfection
   */
  public void setConf(HiveConf conf) {
    this.conf = conf;

    checkQFileTestHack();

    Connection dbConn = null;
    // Set up the JDBC connection pool
    try {
      setupJdbcConnectionPool(conf);
      dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
      determineDatabaseProduct(dbConn);
    } catch (SQLException e) {
      String msg = "Unable to instantiate JDBC connection pooling, " + e.getMessage();
      LOG.error(msg);
      throw new RuntimeException(e);
    }
    finally {
      closeDbConn(dbConn);
    }

    timeout = HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS);
    buildJumpTable();
    retryInterval = HiveConf.getTimeVar(conf, HiveConf.ConfVars.HMSHANDLERINTERVAL,
        TimeUnit.MILLISECONDS);
    retryLimit = HiveConf.getIntVar(conf, HiveConf.ConfVars.HMSHANDLERATTEMPTS);
    deadlockRetryInterval = retryInterval / 10;
  }

  public GetOpenTxnsInfoResponse getOpenTxnsInfo() throws MetaException {
    try {
      // We need to figure out the current transaction number and the list of
      // open transactions.  To avoid needing a transaction on the underlying
      // database we'll look at the current transaction number first.  If it
      // subsequently shows up in the open list that's ok.
      Connection dbConn = null;
      Statement stmt = null;
      ResultSet rs = null;
      try {
        /**
         * This method can run at READ_COMMITTED as long as long as
         * {@link #openTxns(org.apache.hadoop.hive.metastore.api.OpenTxnRequest)} is atomic.
         * More specifically, as long as advancing TransactionID in NEXT_TXN_ID is atomic with
         * adding corresponding entries into TXNS.  The reason is that any txnid below HWM
         * is either in TXNS and thus considered open (Open/Aborted) or it's considered Committed.
         */
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String s = "select ntxn_next - 1 from NEXT_TXN_ID";
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        if (!rs.next()) {
          throw new MetaException("Transaction tables not properly " +
            "initialized, no record found in next_txn_id");
        }
        long hwm = rs.getLong(1);
        if (rs.wasNull()) {
          throw new MetaException("Transaction tables not properly " +
            "initialized, null record found in next_txn_id");
        }
        close(rs);
        List<TxnInfo> txnInfo = new ArrayList<TxnInfo>();
        //need the WHERE clause below to ensure consistent results with READ_COMMITTED
        s = "select txn_id, txn_state, txn_user, txn_host from TXNS where txn_id <= " + hwm;
        LOG.debug("Going to execute query<" + s + ">");
        rs = stmt.executeQuery(s);
        while (rs.next()) {
          char c = rs.getString(2).charAt(0);
          TxnState state;
          switch (c) {
            case TXN_ABORTED:
              state = TxnState.ABORTED;
              break;

            case TXN_OPEN:
              state = TxnState.OPEN;
              break;

            default:
              throw new MetaException("Unexpected transaction state " + c +
                " found in txns table");
          }
          txnInfo.add(new TxnInfo(rs.getLong(1), state, rs.getString(3), rs.getString(4)));
        }
        LOG.debug("Going to rollback");
        dbConn.rollback();
        return new GetOpenTxnsInfoResponse(hwm, txnInfo);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "getOpenTxnsInfo");
        throw new MetaException("Unable to select from transaction database: " + getMessage(e)
          + StringUtils.stringifyException(e));
      } finally {
        close(rs, stmt, dbConn);
      }
    } catch (RetryException e) {
      return getOpenTxnsInfo();
    }
  }

  public GetOpenTxnsResponse getOpenTxns() throws MetaException {
    try {
      // We need to figure out the current transaction number and the list of
      // open transactions.  To avoid needing a transaction on the underlying
      // database we'll look at the current transaction number first.  If it
      // subsequently shows up in the open list that's ok.
      Connection dbConn = null;
      Statement stmt = null;
      ResultSet rs = null;
      try {
        /**
         * This runs at READ_COMMITTED for exactly the same reason as {@link #getOpenTxnsInfo()}
\         */
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String s = "select ntxn_next - 1 from NEXT_TXN_ID";
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        if (!rs.next()) {
          throw new MetaException("Transaction tables not properly " +
            "initialized, no record found in next_txn_id");
        }
        long hwm = rs.getLong(1);
        if (rs.wasNull()) {
          throw new MetaException("Transaction tables not properly " +
            "initialized, null record found in next_txn_id");
        }
        close(rs);
        Set<Long> openList = new HashSet<Long>();
        //need the WHERE clause below to ensure consistent results with READ_COMMITTED
        s = "select txn_id from TXNS where txn_id <= " + hwm;
        LOG.debug("Going to execute query<" + s + ">");
        rs = stmt.executeQuery(s);
        while (rs.next()) {
          openList.add(rs.getLong(1));
        }
        LOG.debug("Going to rollback");
        dbConn.rollback();
        return new GetOpenTxnsResponse(hwm, openList);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "getOpenTxns");
        throw new MetaException("Unable to select from transaction database, "
          + StringUtils.stringifyException(e));
      } finally {
        close(rs, stmt, dbConn);
      }
    } catch (RetryException e) {
      return getOpenTxns();
    }
  }
  public OpenTxnsResponse openTxns(OpenTxnRequest rqst) throws MetaException {
    int numTxns = rqst.getNum_txns();
    try {
      Connection dbConn = null;
      Statement stmt = null;
      ResultSet rs = null;
      try {
        lockInternal();
        /**
         * To make {@link #getOpenTxns()}/{@link #getOpenTxnsInfo()} work correctly, this operation must ensure
         * that advancing the counter in NEXT_TXN_ID and adding appropriate entries to TXNS is atomic.
         * Also, advancing the counter must work when multiple metastores are running.
         * SELECT ... FOR UPDATE is used to prevent
         * concurrent DB transactions being rolled back due to Write-Write conflict on NEXT_TXN_ID.
         *
         * In the current design, there can be several metastore instances running in a given Warehouse.
         * This makes ideas like reserving a range of IDs to save trips to DB impossible.  For example,
         * a client may go to MS1 and start a transaction with ID 500 to update a particular row.
         * Now the same client will start another transaction, except it ends up on MS2 and may get
         * transaction ID 400 and update the same row.  Now the merge that happens to materialize the snapshot
         * on read will thing the version of the row from transaction ID 500 is the latest one.
         *
         * Longer term we can consider running Active-Passive MS (at least wrt to ACID operations).  This
         * set could support a write-through cache for added performance.
         */
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        // Make sure the user has not requested an insane amount of txns.
        int maxTxns = HiveConf.getIntVar(conf,
          HiveConf.ConfVars.HIVE_TXN_MAX_OPEN_BATCH);
        if (numTxns > maxTxns) numTxns = maxTxns;

        stmt = dbConn.createStatement();
        String s = addForUpdateClause("select ntxn_next from NEXT_TXN_ID");
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        if (!rs.next()) {
          throw new MetaException("Transaction database not properly " +
            "configured, can't find next transaction id.");
        }
        long first = rs.getLong(1);
        s = "update NEXT_TXN_ID set ntxn_next = " + (first + numTxns);
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);

        long now = getDbTime(dbConn);
        List<Long> txnIds = new ArrayList<Long>(numTxns);
        ArrayList<String> queries = new ArrayList<String>();
        String query;
        String insertClause = "insert into TXNS (txn_id, txn_state, txn_started, txn_last_heartbeat, txn_user, txn_host) values ";
        StringBuilder valuesClause = new StringBuilder();

        for (long i = first; i < first + numTxns; i++) {
          txnIds.add(i);

          if (i > first &&
              (i - first) % conf.getIntVar(HiveConf.ConfVars.METASTORE_DIRECT_SQL_MAX_ELEMENTS_VALUES_CLAUSE) == 0) {
            // wrap up the current query, and start a new one
            query = insertClause + valuesClause.toString();
            queries.add(query);

            valuesClause.setLength(0);
            valuesClause.append("(").append(i).append(", 'o', ").append(now).append(", ").append(now)
                .append(", '").append(rqst.getUser()).append("', '").append(rqst.getHostname())
                .append("')");

            continue;
          }

          if (i > first) {
            valuesClause.append(", ");
          }

          valuesClause.append("(").append(i).append(", 'o', ").append(now).append(", ").append(now)
              .append(", '").append(rqst.getUser()).append("', '").append(rqst.getHostname())
              .append("')");
        }

        query = insertClause + valuesClause.toString();
        queries.add(query);

        for (String q : queries) {
          LOG.debug("Going to execute update <" + q + ">");
          stmt.execute(q);
        }
        LOG.debug("Going to commit");
        dbConn.commit();
        return new OpenTxnsResponse(txnIds);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "openTxns(" + rqst + ")");
        throw new MetaException("Unable to select from transaction database "
          + StringUtils.stringifyException(e));
      } finally {
        close(rs, stmt, dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      return openTxns(rqst);
    }
  }

  public void abortTxn(AbortTxnRequest rqst) throws NoSuchTxnException, MetaException {
    long txnid = rqst.getTxnid();
    try {
      Connection dbConn = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        if (abortTxns(dbConn, Collections.singletonList(txnid)) != 1) {
          LOG.debug("Going to rollback");
          dbConn.rollback();
          throw new NoSuchTxnException("No such transaction " + JavaUtils.txnIdToString(txnid));
        }

        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "abortTxn(" + rqst + ")");
        throw new MetaException("Unable to update transaction database "
          + StringUtils.stringifyException(e));
      } finally {
        closeDbConn(dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      abortTxn(rqst);
    }
  }

  public void commitTxn(CommitTxnRequest rqst)
    throws NoSuchTxnException, TxnAbortedException,  MetaException {
    long txnid = rqst.getTxnid();
    try {
      Connection dbConn = null;
      Statement stmt = null;
      ResultSet lockHandle = null;
      try {
        lockInternal();
        /**
         * Runs at READ_COMMITTED with S4U on TXNS row for "txnid".  S4U ensures that no other
         * operation can change this txn (such acquiring locks). While lock() and commitTxn()
         * should not normally run concurrently (for same txn) but could due to bugs in the client
         * which could then corrupt internal transaction manager state.  Also competes with abortTxn().
         */
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        lockHandle = lockTransactionRecord(stmt, txnid, TXN_OPEN);
        if(lockHandle == null) {
          //this also ensures that txn is still there and in expected state (hasn't been timed out)
          ensureValidTxn(dbConn, txnid, stmt);
          shouldNeverHappen(txnid);
        }

        // Move the record from txn_components into completed_txn_components so that the compactor
        // knows where to look to compact.
        String s = "insert into COMPLETED_TXN_COMPONENTS select tc_txnid, tc_database, tc_table, " +
          "tc_partition from TXN_COMPONENTS where tc_txnid = " + txnid;
        LOG.debug("Going to execute insert <" + s + ">");
        if (stmt.executeUpdate(s) < 1) {
          //this can be reasonable for an empty txn START/COMMIT
          LOG.info("Expected to move at least one record from txn_components to " +
            "completed_txn_components when committing txn! " + JavaUtils.txnIdToString(txnid));
        }

        // Always access TXN_COMPONENTS before HIVE_LOCKS;
        s = "delete from TXN_COMPONENTS where tc_txnid = " + txnid;
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);
        // Always access HIVE_LOCKS before TXNS
        s = "delete from HIVE_LOCKS where hl_txnid = " + txnid;
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);
        s = "delete from TXNS where txn_id = " + txnid;
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "commitTxn(" + rqst + ")");
        throw new MetaException("Unable to update transaction database "
          + StringUtils.stringifyException(e));
      } finally {
        close(lockHandle, stmt, dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      commitTxn(rqst);
    }
  }

  /**
   * As much as possible (i.e. in absence of retries) we want both operations to be done on the same
   * connection (but separate transactions).  This avoid some flakiness in BONECP where if you
   * perform an operation on 1 connection and immediately get another fron the pool, the 2nd one
   * doesn't see results of the first.
   */
  public LockResponse lock(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException, MetaException {
    ConnectionLockIdPair connAndLockId = enqueueLockWithRetry(rqst);
    try {
      return checkLockWithRetry(connAndLockId.dbConn, connAndLockId.extLockId, rqst.getTxnid());
    }
    catch(NoSuchLockException e) {
      // This should never happen, as we just added the lock id
      throw new MetaException("Couldn't find a lock we just created! " + e.getMessage());
    }
  }
  private static final class ConnectionLockIdPair {
    private final Connection dbConn;
    private final long extLockId;
    private ConnectionLockIdPair(Connection dbConn, long extLockId) {
      this.dbConn = dbConn;
      this.extLockId = extLockId;
    }
  }

  /**
   * Note that by definition select for update is divorced from update, i.e. you executeQuery() to read
   * and then executeUpdate().  One other alternative would be to actually update the row in TXNX but
   * to the same value as before thus forcing db to acquire write lock for duration of the transaction.
   *
   * There is no real reason to return the ResultSet here other than to make sure the reference to it
   * is retained for duration of intended lock scope and is not GC'd thus (unlikely) causing lock
   * to be released.
   * @param txnState the state this txn is expected to be in.  may be null
   * @return null if no row was found
   * @throws SQLException
   * @throws MetaException
   */
  private ResultSet lockTransactionRecord(Statement stmt, long txnId, Character txnState) throws SQLException, MetaException {
    String query = "select TXN_STATE from TXNS where TXN_ID = " + txnId + (txnState != null ? " AND TXN_STATE=" + quoteChar(txnState) : "");
    ResultSet rs = stmt.executeQuery(addForUpdateClause(query));
    if(rs.next()) {
      return rs;
    }
    close(rs);
    return null;
  }

  /**
   * This enters locks into the queue in {@link #LOCK_WAITING} mode.
   *
   * Isolation Level Notes:
   * 1. We use S4U (withe read_committed) to generate the next (ext) lock id.  This serializes
   * any 2 {@code enqueueLockWithRetry()} calls.
   * 2. We use S4U on the relevant TXNS row to block any concurrent abort/commit/etc operations
   * @see #checkLockWithRetry(Connection, long, long)
   */
  private ConnectionLockIdPair enqueueLockWithRetry(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException, MetaException {
    boolean success = false;
    Connection dbConn = null;
    try {
      Statement stmt = null;
      ResultSet rs = null;
      ResultSet lockHandle = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        long txnid = rqst.getTxnid();
        stmt = dbConn.createStatement();
        if (isValidTxn(txnid)) {
          //this also ensures that txn is still there in expected state
          lockHandle = lockTransactionRecord(stmt, txnid, TXN_OPEN);
          if(lockHandle == null) {
            ensureValidTxn(dbConn, txnid, stmt);
            shouldNeverHappen(txnid);
          }
        }
        /** Get the next lock id.
         * This has to be atomic with adding entries to HIVE_LOCK entries (1st add in W state) to prevent a race.
         * Suppose ID gen is a separate txn and 2 concurrent lock() methods are running.  1st one generates nl_next=7,
         * 2nd nl_next=8.  Then 8 goes first to insert into HIVE_LOCKS and aquires the locks.  Then 7 unblocks,
         * and add it's W locks but it won't see locks from 8 since to be 'fair' {@link #checkLock(java.sql.Connection, long)}
         * doesn't block on locks acquired later than one it's checking*/
        String s = addForUpdateClause("select nl_next from NEXT_LOCK_ID");
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        if (!rs.next()) {
          LOG.debug("Going to rollback");
          dbConn.rollback();
          throw new MetaException("Transaction tables not properly " +
            "initialized, no record found in next_lock_id");
        }
        long extLockId = rs.getLong(1);
        s = "update NEXT_LOCK_ID set nl_next = " + (extLockId + 1);
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);

        if (txnid > 0) {
          // For each component in this lock request,
          // add an entry to the txn_components table
          // This must be done before HIVE_LOCKS is accessed
          for (LockComponent lc : rqst.getComponent()) {
            String dbName = lc.getDbname();
            String tblName = lc.getTablename();
            String partName = lc.getPartitionname();
            s = "insert into TXN_COMPONENTS " +
              "(tc_txnid, tc_database, tc_table, tc_partition) " +
              "values (" + txnid + ", '" + dbName + "', " +
              (tblName == null ? "null" : "'" + tblName + "'") + ", " +
              (partName == null ? "null" : "'" + partName + "'") + ")";
            LOG.debug("Going to execute update <" + s + ">");
            stmt.executeUpdate(s);
          }
        }

        long intLockId = 0;
        for (LockComponent lc : rqst.getComponent()) {
          intLockId++;
          String dbName = lc.getDbname();
          String tblName = lc.getTablename();
          String partName = lc.getPartitionname();
          LockType lockType = lc.getType();
          char lockChar = 'z';
          switch (lockType) {
            case EXCLUSIVE:
              lockChar = LOCK_EXCLUSIVE;
              break;
            case SHARED_READ:
              lockChar = LOCK_SHARED;
              break;
            case SHARED_WRITE:
              lockChar = LOCK_SEMI_SHARED;
              break;
          }
          long now = getDbTime(dbConn);
          s = "insert into HIVE_LOCKS " +
            " (hl_lock_ext_id, hl_lock_int_id, hl_txnid, hl_db, hl_table, " +
            "hl_partition, hl_lock_state, hl_lock_type, hl_last_heartbeat, hl_user, hl_host)" +
            " values (" + extLockId + ", " +
            +intLockId + "," + txnid + ", '" +
            dbName + "', " + (tblName == null ? "null" : "'" + tblName + "'")
            + ", " + (partName == null ? "null" : "'" + partName + "'") +
            ", '" + LOCK_WAITING + "', " + "'" + lockChar + "', " +
            //for locks associated with a txn, we always heartbeat txn and timeout based on that
            (isValidTxn(txnid) ? 0 : now) + ", '" +
            rqst.getUser() + "', '" + rqst.getHostname() + "')";
          LOG.debug("Going to execute update <" + s + ">");
          stmt.executeUpdate(s);
        }
        dbConn.commit();
        success = true;
        return new ConnectionLockIdPair(dbConn, extLockId);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "enqueueLockWithRetry(" + rqst + ")");
        throw new MetaException("Unable to update transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        close(lockHandle);
        close(rs, stmt, null);
        if (!success) {
          /* This needs to return a "live" connection to be used by operation that follows it.
          Thus it only closes Connection on failure/retry. */
          closeDbConn(dbConn);
        }
        unlockInternal();
      }
    }
    catch(RetryException e) {
      return enqueueLockWithRetry(rqst);
    }
  }
  private LockResponse checkLockWithRetry(Connection dbConn, long extLockId, long txnId)
    throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, MetaException {
    try {
      try {
        lockInternal();
        if(dbConn.isClosed()) {
          //should only get here if retrying this op
          dbConn = getDbConn(Connection.TRANSACTION_SERIALIZABLE);
        }
        dbConn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        return checkLock(dbConn, extLockId);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "checkLockWithRetry(" + extLockId + "," + txnId + ")");
        throw new MetaException("Unable to update transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        unlockInternal();
        closeDbConn(dbConn);
      }
    }
    catch(RetryException e) {
      return checkLockWithRetry(dbConn, extLockId, txnId);
    }
  }
  /**
   * Why doesn't this get a txnid as parameter?  The caller should either know the txnid or know there isn't one.
   * Either way getTxnIdFromLockId() will not be needed.  This would be a Thrift change.
   *
   * Also, when lock acquisition returns WAITING, it's retried every 15 seconds (best case, see DbLockManager.backoff(),
   * in practice more often)
   * which means this is heartbeating way more often than hive.txn.timeout and creating extra load on DB.
   *
   * The clients that operate in blocking mode, can't heartbeat a lock until the lock is acquired.
   * We should make CheckLockRequest include timestamp or last request to skip unnecessary heartbeats. Thrift change.
   *
   * {@link #checkLock(java.sql.Connection, long)}  must run at SERIALIZABLE (make sure some lock we are checking
   * against doesn't move from W to A in another txn) but this method can heartbeat in
   * separate txn at READ_COMMITTED.
   */
  public LockResponse checkLock(CheckLockRequest rqst)
    throws NoSuchTxnException, NoSuchLockException, TxnAbortedException, MetaException {
    try {
      Connection dbConn = null;
      long extLockId = rqst.getLockid();
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        // Heartbeat on the lockid first, to assure that our lock is still valid.
        // Then look up the lock info (hopefully in the cache).  If these locks
        // are associated with a transaction then heartbeat on that as well.
        LockInfo info = getTxnIdFromLockId(dbConn, extLockId);
        if(info == null) {
          throw new NoSuchLockException("No such lock " + JavaUtils.lockIdToString(extLockId));
        }
        if (info.txnId > 0) {
          heartbeatTxn(dbConn, info.txnId);
        }
        else {
          heartbeatLock(dbConn, extLockId);
        }
        //todo: strictly speaking there is a bug here.  heartbeat*() commits but both heartbeat and
        //checkLock() are in the same retry block, so if checkLock() throws, heartbeat is also retired
        //extra heartbeat is logically harmless, but ...
        dbConn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        return checkLock(dbConn, extLockId);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "checkLock(" + rqst + " )");
        throw new MetaException("Unable to update transaction database " +
          JavaUtils.lockIdToString(extLockId) + " " + StringUtils.stringifyException(e));
      } finally {
        closeDbConn(dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      return checkLock(rqst);
    }

  }

  /**
   * This would have been made simpler if all locks were associated with a txn.  Then only txn needs to
   * be heartbeated, committed, etc.  no need for client to track individual locks.
   * When removing locks not associated with txn this potentially conflicts with
   * heartbeat/performTimeout which are update/delete of HIVE_LOCKS thus will be locked as needed by db.
   * since this only removes from HIVE_LOCKS at worst some lock acquire is delayed
   */
  public void unlock(UnlockRequest rqst)
    throws NoSuchLockException, TxnOpenException, MetaException {
    try {
      Connection dbConn = null;
      Statement stmt = null;
      long extLockId = rqst.getLockid();
      try {
        /**
         * This method is logically like commit for read-only auto commit queries.
         * READ_COMMITTED since this only has 1 delete statement and no new entries with the
         * same hl_lock_ext_id can be added, i.e. all rows with a given hl_lock_ext_id are
         * created in a single atomic operation.
         * Theoretically, this competes with {@link #lock(org.apache.hadoop.hive.metastore.api.LockRequest)}
         * but hl_lock_ext_id is not known until that method returns.
         * Also competes with {@link #checkLock(org.apache.hadoop.hive.metastore.api.CheckLockRequest)}
         * but using SERIALIZABLE doesn't materially change the interaction.
         * If "delete" stmt misses, additional logic is best effort to produce meaningful error msg.
         */
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        //hl_txnid <> 0 means it's associated with a transaction
        String s = "delete from HIVE_LOCKS where hl_lock_ext_id = " + extLockId + " AND (hl_txnid = 0 OR" +
          " (hl_txnid <> 0 AND hl_lock_state = '" + LOCK_WAITING + "'))";
        //(hl_txnid <> 0 AND hl_lock_state = '" + LOCK_WAITING + "') is for multi-statement txns where
        //some query attempted to lock (thus LOCK_WAITING state) but is giving up due to timeout for example
        LOG.debug("Going to execute update <" + s + ">");
        int rc = stmt.executeUpdate(s);
        if (rc < 1) {
          LOG.debug("Going to rollback");
          dbConn.rollback();
          LockInfo info = getTxnIdFromLockId(dbConn, extLockId);
          if(info == null) {
            //didn't find any lock with extLockId but at ReadCommitted there is a possibility that
            //it existed when above delete ran but it didn't have the expected state.
            LOG.error("No lock in " + LOCK_WAITING + " mode found for unlock(" + rqst + ")");
            throw new NoSuchLockException("No such lock " + JavaUtils.lockIdToString(extLockId));
          }
          if(info.txnId != 0) {
            String msg = "Unlocking locks associated with transaction not permitted.  " + info;
            LOG.error(msg);
            throw new TxnOpenException(msg);
          }
          if(info.txnId == 0) {
            //we didn't see this lock when running DELETE stmt above but now it showed up
            //so should "should never happen" happened...
            String msg = "Found lock in unexpected state " + info;
            LOG.error(msg);
            throw new MetaException(msg);
          }
        }
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "unlock(" + rqst + ")");
        throw new MetaException("Unable to update transaction database " +
          JavaUtils.lockIdToString(extLockId) + " " + StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      unlock(rqst);
    }
  }

  /**
   * used to sort entries in {@link org.apache.hadoop.hive.metastore.api.ShowLocksResponse}
   */
  private static class LockInfoExt extends LockInfo {
    private final ShowLocksResponseElement e;
    LockInfoExt(ShowLocksResponseElement e) {
      super(e);
      this.e = e;
    }
  }
  public ShowLocksResponse showLocks(ShowLocksRequest rqst) throws MetaException {
    try {
      Connection dbConn = null;
      ShowLocksResponse rsp = new ShowLocksResponse();
      List<ShowLocksResponseElement> elems = new ArrayList<ShowLocksResponseElement>();
      List<LockInfoExt> sortedList = new ArrayList<LockInfoExt>();
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        String s = "select hl_lock_ext_id, hl_txnid, hl_db, hl_table, hl_partition, hl_lock_state, " +
          "hl_lock_type, hl_last_heartbeat, hl_acquired_at, hl_user, hl_host, hl_lock_int_id," +
          "hl_blockedby_ext_id, hl_blockedby_int_id from HIVE_LOCKS";
        LOG.debug("Doing to execute query <" + s + ">");
        ResultSet rs = stmt.executeQuery(s);
        while (rs.next()) {
          ShowLocksResponseElement e = new ShowLocksResponseElement();
          e.setLockid(rs.getLong(1));
          long txnid = rs.getLong(2);
          if (!rs.wasNull()) e.setTxnid(txnid);
          e.setDbname(rs.getString(3));
          e.setTablename(rs.getString(4));
          String partition = rs.getString(5);
          if (partition != null) e.setPartname(partition);
          switch (rs.getString(6).charAt(0)) {
            case LOCK_ACQUIRED: e.setState(LockState.ACQUIRED); break;
            case LOCK_WAITING: e.setState(LockState.WAITING); break;
            default: throw new MetaException("Unknown lock state " + rs.getString(6).charAt(0));
          }
          switch (rs.getString(7).charAt(0)) {
            case LOCK_SEMI_SHARED: e.setType(LockType.SHARED_WRITE); break;
            case LOCK_EXCLUSIVE: e.setType(LockType.EXCLUSIVE); break;
            case LOCK_SHARED: e.setType(LockType.SHARED_READ); break;
            default: throw new MetaException("Unknown lock type " + rs.getString(6).charAt(0));
          }
          e.setLastheartbeat(rs.getLong(8));
          long acquiredAt = rs.getLong(9);
          if (!rs.wasNull()) e.setAcquiredat(acquiredAt);
          e.setUser(rs.getString(10));
          e.setHostname(rs.getString(11));
          e.setLockIdInternal(rs.getLong(12));
          long id = rs.getLong(13);
          if(!rs.wasNull()) {
            e.setBlockedByExtId(id);
          }
          id = rs.getLong(14);
          if(!rs.wasNull()) {
            e.setBlockedByIntId(id);
          }
          sortedList.add(new LockInfoExt(e));
        }
        LOG.debug("Going to rollback");
        dbConn.rollback();
      } catch (SQLException e) {
        checkRetryable(dbConn, e, "showLocks(" + rqst + ")");
        throw new MetaException("Unable to select from transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
      //this ensures that "SHOW LOCKS" prints the locks in the same order as they are examined
      //by checkLock() - makes diagnostics easier.
      Collections.sort(sortedList, new LockInfoComparator());
      for(LockInfoExt lockInfoExt : sortedList) {
        elems.add(lockInfoExt.e);
      }
      rsp.setLocks(elems);
      return rsp;
    } catch (RetryException e) {
      return showLocks(rqst);
    }
  }

  /**
   * {@code ids} should only have txnid or lockid but not both, ideally.
   * Currently DBTxnManager.heartbeat() enforces this.
   */
  public void heartbeat(HeartbeatRequest ids)
    throws NoSuchTxnException,  NoSuchLockException, TxnAbortedException, MetaException {
    try {
      Connection dbConn = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        heartbeatLock(dbConn, ids.getLockid());
        heartbeatTxn(dbConn, ids.getTxnid());
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "heartbeat(" + ids + ")");
        throw new MetaException("Unable to select from transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      heartbeat(ids);
    }
  }

  public HeartbeatTxnRangeResponse heartbeatTxnRange(HeartbeatTxnRangeRequest rqst)
    throws MetaException {
    try {
      Connection dbConn = null;
      HeartbeatTxnRangeResponse rsp = new HeartbeatTxnRangeResponse();
      Set<Long> nosuch = new HashSet<Long>();
      Set<Long> aborted = new HashSet<Long>();
      rsp.setNosuch(nosuch);
      rsp.setAborted(aborted);
      try {
        /**
         * READ_COMMITTED is sufficient since {@link #heartbeatTxn(java.sql.Connection, long)}
         * only has 1 update statement in it and
         * we only update existing txns, i.e. nothing can add additional txns that this operation
         * would care about (which would have required SERIALIZABLE)
         */
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        for (long txn = rqst.getMin(); txn <= rqst.getMax(); txn++) {
          try {
            //todo: do all updates in 1 SQL statement and check update count
            //if update count is less than was requested, go into more expensive checks
            //for each txn
            heartbeatTxn(dbConn, txn);
          } catch (NoSuchTxnException e) {
            nosuch.add(txn);
          } catch (TxnAbortedException e) {
            aborted.add(txn);
          }
        }
        return rsp;
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "heartbeatTxnRange(" + rqst + ")");
        throw new MetaException("Unable to select from transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      return heartbeatTxnRange(rqst);
    }
  }

  public long compact(CompactionRequest rqst) throws MetaException {
    // Put a compaction request in the queue.
    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        // Get the id for the next entry in the queue
        String s = addForUpdateClause("select ncq_next from NEXT_COMPACTION_QUEUE_ID");
        LOG.debug("going to execute query <" + s + ">");
        ResultSet rs = stmt.executeQuery(s);
        if (!rs.next()) {
          LOG.debug("Going to rollback");
          dbConn.rollback();
          throw new MetaException("Transaction tables not properly initiated, " +
            "no record found in next_compaction_queue_id");
        }
        long id = rs.getLong(1);
        s = "update NEXT_COMPACTION_QUEUE_ID set ncq_next = " + (id + 1);
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);

        StringBuilder buf = new StringBuilder("insert into COMPACTION_QUEUE (cq_id, cq_database, " +
          "cq_table, ");
        String partName = rqst.getPartitionname();
        if (partName != null) buf.append("cq_partition, ");
        buf.append("cq_state, cq_type");
        if (rqst.getRunas() != null) buf.append(", cq_run_as");
        buf.append(") values (");
        buf.append(id);
        buf.append(", '");
        buf.append(rqst.getDbname());
        buf.append("', '");
        buf.append(rqst.getTablename());
        buf.append("', '");
        if (partName != null) {
          buf.append(partName);
          buf.append("', '");
        }
        buf.append(INITIATED_STATE);
        buf.append("', '");
        switch (rqst.getType()) {
          case MAJOR:
            buf.append(MAJOR_TYPE);
            break;

          case MINOR:
            buf.append(MINOR_TYPE);
            break;

          default:
            LOG.debug("Going to rollback");
            dbConn.rollback();
            throw new MetaException("Unexpected compaction type " + rqst.getType().toString());
        }
        if (rqst.getRunas() != null) {
          buf.append("', '");
          buf.append(rqst.getRunas());
        }
        buf.append("')");
        s = buf.toString();
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);
        LOG.debug("Going to commit");
        dbConn.commit();
        return id;
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "compact(" + rqst + ")");
        throw new MetaException("Unable to select from transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      return compact(rqst);
    }
  }

  public ShowCompactResponse showCompact(ShowCompactRequest rqst) throws MetaException {
    ShowCompactResponse response = new ShowCompactResponse(new ArrayList<ShowCompactResponseElement>());
    Connection dbConn = null;
    Statement stmt = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String s = "select cq_database, cq_table, cq_partition, cq_state, cq_type, cq_worker_id, " +
          "cq_start, -1 cc_end, cq_run_as, cq_hadoop_job_id, cq_id from COMPACTION_QUEUE union all " +
          "select cc_database, cc_table, cc_partition, cc_state, cc_type, cc_worker_id, " +
          "cc_start, cc_end, cc_run_as, cc_hadoop_job_id, cc_id from COMPLETED_COMPACTIONS";
        //what I want is order by cc_end desc, cc_start asc (but derby has a bug https://issues.apache.org/jira/browse/DERBY-6013)
        //to sort so that currently running jobs are at the end of the list (bottom of screen)
        //and currently running ones are in sorted by start time
        //w/o order by likely currently running compactions will be first (LHS of Union)
        LOG.debug("Going to execute query <" + s + ">");
        ResultSet rs = stmt.executeQuery(s);
        while (rs.next()) {
          ShowCompactResponseElement e = new ShowCompactResponseElement();
          e.setDbname(rs.getString(1));
          e.setTablename(rs.getString(2));
          e.setPartitionname(rs.getString(3));
          switch (rs.getString(4).charAt(0)) {
            case INITIATED_STATE: e.setState(INITIATED_RESPONSE); break;
            case WORKING_STATE: e.setState(WORKING_RESPONSE); break;
            case READY_FOR_CLEANING: e.setState(CLEANING_RESPONSE); break;
            case FAILED_STATE: e.setState(FAILED_RESPONSE); break;
            case SUCCEEDED_STATE: e.setState(SUCCEEDED_RESPONSE); break;
            default:
              //do nothing to handle RU/D if we add another status
          }
          switch (rs.getString(5).charAt(0)) {
            case MAJOR_TYPE: e.setType(CompactionType.MAJOR); break;
            case MINOR_TYPE: e.setType(CompactionType.MINOR); break;
            default:
              //do nothing to handle RU/D if we add another status
          }
          e.setWorkerid(rs.getString(6));
          e.setStart(rs.getLong(7));
          long endTime = rs.getLong(8);
          if(endTime != -1) {
            e.setEndTime(endTime);
          }
          e.setRunAs(rs.getString(9));
          e.setHadoopJobId(rs.getString(10));
          long id = rs.getLong(11);//for debugging
          response.addToCompacts(e);
        }
        LOG.debug("Going to rollback");
        dbConn.rollback();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "showCompact(" + rqst + ")");
        throw new MetaException("Unable to select from transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
      return response;
    } catch (RetryException e) {
      return showCompact(rqst);
    }
  }

  private static void shouldNeverHappen(long txnid) {
    throw new RuntimeException("This should never happen: " + JavaUtils.txnIdToString(txnid));
  }
  private static void shouldNeverHappen(long txnid, long extLockId, long intLockId) {
    throw new RuntimeException("This should never happen: " + JavaUtils.txnIdToString(txnid) + " "
      + JavaUtils.lockIdToString(extLockId) + " " + intLockId);
  }
  public void addDynamicPartitions(AddDynamicPartitions rqst)
      throws NoSuchTxnException,  TxnAbortedException, MetaException {
    Connection dbConn = null;
    Statement stmt = null;
    ResultSet lockHandle = null;
    try {
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        lockHandle = lockTransactionRecord(stmt, rqst.getTxnid(), TXN_OPEN);
        if(lockHandle == null) {
          //ensures txn is still there and in expected state
          ensureValidTxn(dbConn, rqst.getTxnid(), stmt);
          shouldNeverHappen(rqst.getTxnid());
        }
        for (String partName : rqst.getPartitionnames()) {
          StringBuilder buff = new StringBuilder();
          buff.append("insert into TXN_COMPONENTS (tc_txnid, tc_database, tc_table, tc_partition) values (");
          buff.append(rqst.getTxnid());
          buff.append(", '");
          buff.append(rqst.getDbname());
          buff.append("', '");
          buff.append(rqst.getTablename());
          buff.append("', '");
          buff.append(partName);
          buff.append("')");
          String s = buff.toString();
          LOG.debug("Going to execute update <" + s + ">");
          stmt.executeUpdate(s);
        }
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "addDynamicPartitions(" + rqst + ")");
        throw new MetaException("Unable to insert into from transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        close(lockHandle, stmt, dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      addDynamicPartitions(rqst);
    }
  }

  /**
   * Clean up corresponding records in metastore tables, specifically:
   * TXN_COMPONENTS, COMPLETED_TXN_COMPONENTS, COMPACTION_QUEUE, COMPLETED_COMPACTIONS
   */
  public void cleanupRecords(HiveObjectType type, Database db, Table table,
                             Iterator<Partition> partitionIterator) throws MetaException {
    try {
      Connection dbConn = null;
      Statement stmt = null;

      try {
        String dbName;
        String tblName;
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        List<String> queries = new ArrayList<String>();
        StringBuilder buff = new StringBuilder();

        switch (type) {
          case DATABASE:
            dbName = db.getName();

            buff.append("delete from TXN_COMPONENTS where tc_database='");
            buff.append(dbName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from COMPLETED_TXN_COMPONENTS where ctc_database='");
            buff.append(dbName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from COMPACTION_QUEUE where cq_database='");
            buff.append(dbName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from COMPLETED_COMPACTIONS where cc_database='");
            buff.append(dbName);
            buff.append("'");
            queries.add(buff.toString());

            break;
          case TABLE:
            dbName = table.getDbName();
            tblName = table.getTableName();

            buff.append("delete from TXN_COMPONENTS where tc_database='");
            buff.append(dbName);
            buff.append("' and tc_table='");
            buff.append(tblName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from COMPLETED_TXN_COMPONENTS where ctc_database='");
            buff.append(dbName);
            buff.append("' and ctc_table='");
            buff.append(tblName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from COMPACTION_QUEUE where cq_database='");
            buff.append(dbName);
            buff.append("' and cq_table='");
            buff.append(tblName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from COMPLETED_COMPACTIONS where cc_database='");
            buff.append(dbName);
            buff.append("' and cc_table='");
            buff.append(tblName);
            buff.append("'");
            queries.add(buff.toString());

            break;
          case PARTITION:
            dbName = table.getDbName();
            tblName = table.getTableName();
            List<FieldSchema> partCols = table.getPartitionKeys();  // partition columns
            List<String> partVals;                                  // partition values
            String partName;

            while (partitionIterator.hasNext()) {
              Partition p = partitionIterator.next();
              partVals = p.getValues();
              partName = Warehouse.makePartName(partCols, partVals);

              buff.append("delete from TXN_COMPONENTS where tc_database='");
              buff.append(dbName);
              buff.append("' and tc_table='");
              buff.append(tblName);
              buff.append("' and tc_partition='");
              buff.append(partName);
              buff.append("'");
              queries.add(buff.toString());

              buff.setLength(0);
              buff.append("delete from COMPLETED_TXN_COMPONENTS where ctc_database='");
              buff.append(dbName);
              buff.append("' and ctc_table='");
              buff.append(tblName);
              buff.append("' and ctc_partition='");
              buff.append(partName);
              buff.append("'");
              queries.add(buff.toString());

              buff.setLength(0);
              buff.append("delete from COMPACTION_QUEUE where cq_database='");
              buff.append(dbName);
              buff.append("' and cq_table='");
              buff.append(tblName);
              buff.append("' and cq_partition='");
              buff.append(partName);
              buff.append("'");
              queries.add(buff.toString());

              buff.setLength(0);
              buff.append("delete from COMPLETED_COMPACTIONS where cc_database='");
              buff.append(dbName);
              buff.append("' and cc_table='");
              buff.append(tblName);
              buff.append("' and cc_partition='");
              buff.append(partName);
              buff.append("'");
              queries.add(buff.toString());
            }

            break;
          default:
            throw new MetaException("Invalid object type for cleanup: " + type);
        }

        for (String query : queries) {
          LOG.debug("Going to execute update <" + query + ">");
          stmt.executeUpdate(query);
        }

        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "cleanupRecords");
        if (e.getMessage().contains("does not exist")) {
          LOG.warn("Cannot perform cleanup since metastore table does not exist");
        } else {
          throw new MetaException("Unable to clean up " + StringUtils.stringifyException(e));
        }
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      cleanupRecords(type, db, table, partitionIterator);
    }
  }

  /**
   * For testing only, do not use.
   */
  @VisibleForTesting
  public int numLocksInLockTable() throws SQLException, MetaException {
    Connection dbConn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
      stmt = dbConn.createStatement();
      String s = "select count(*) from HIVE_LOCKS";
      LOG.debug("Going to execute query <" + s + ">");
      rs = stmt.executeQuery(s);
      rs.next();
      int rc = rs.getInt(1);
      // Necessary to clean up the transaction in the db.
      dbConn.rollback();
      return rc;
    } finally {
      close(rs, stmt, dbConn);
    }
  }

  /**
   * For testing only, do not use.
   */
  public long setTimeout(long milliseconds) {
    long previous_timeout = timeout;
    timeout = milliseconds;
    return previous_timeout;
  }

  protected class RetryException extends Exception {

  }

  protected Connection getDbConn(int isolationLevel) throws SQLException {
    int rc = doRetryOnConnPool ? 10 : 1;
    Connection dbConn = null;
    while (true) {
      try {
        dbConn = connPool.getConnection();
        dbConn.setAutoCommit(false);
        dbConn.setTransactionIsolation(isolationLevel);
        return dbConn;
      } catch (SQLException e){
        closeDbConn(dbConn);
        if ((--rc) <= 0) throw e;
        LOG.error("There is a problem with a connection from the pool, retrying(rc=" + rc + "): " +
          getMessage(e), e);
      }
    }
  }

  static void rollbackDBConn(Connection dbConn) {
    try {
      if (dbConn != null && !dbConn.isClosed()) dbConn.rollback();
    } catch (SQLException e) {
      LOG.warn("Failed to rollback db connection " + getMessage(e));
    }
  }
  protected static void closeDbConn(Connection dbConn) {
    try {
      if (dbConn != null && !dbConn.isClosed()) {
        dbConn.close();
      }
    } catch (SQLException e) {
      LOG.warn("Failed to close db connection " + getMessage(e));
    }
  }

  /**
   * Close statement instance.
   * @param stmt statement instance.
   */
  protected static void closeStmt(Statement stmt) {
    try {
      if (stmt != null && !stmt.isClosed()) stmt.close();
    } catch (SQLException e) {
      LOG.warn("Failed to close statement " + getMessage(e));
    }
  }

  /**
   * Close the ResultSet.
   * @param rs may be {@code null}
   */
  static void close(ResultSet rs) {
    try {
      if (rs != null && !rs.isClosed()) {
        rs.close();
      }
    }
    catch(SQLException ex) {
      LOG.warn("Failed to close statement " + getMessage(ex));
    }
  }

  /**
   * Close all 3 JDBC artifacts in order: {@code rs stmt dbConn}
   */
  static void close(ResultSet rs, Statement stmt, Connection dbConn) {
    close(rs);
    closeStmt(stmt);
    closeDbConn(dbConn);
  }
  /**
   * Determine if an exception was such that it makes sense to retry.  Unfortunately there is no standard way to do
   * this, so we have to inspect the error messages and catch the telltale signs for each
   * different database.  This method will throw {@code RetryException}
   * if the error is retry-able.
   * @param conn database connection
   * @param e exception that was thrown.
   * @param caller name of the method calling this (and other info useful to log)
   * @throws org.apache.hadoop.hive.metastore.txn.TxnHandler.RetryException when the operation should be retried
   */
  protected void checkRetryable(Connection conn,
                                SQLException e,
                                String caller) throws RetryException, MetaException {

    // If you change this function, remove the @Ignore from TestTxnHandler.deadlockIsDetected()
    // to test these changes.
    // MySQL and MSSQL use 40001 as the state code for rollback.  Postgres uses 40001 and 40P01.
    // Oracle seems to return different SQLStates and messages each time,
    // so I've tried to capture the different error messages (there appear to be fewer different
    // error messages than SQL states).
    // Derby and newer MySQL driver use the new SQLTransactionRollbackException
    boolean sendRetrySignal = false;
    try {
      if(dbProduct == null) {
        throw new IllegalStateException("DB Type not determined yet.");
      }
      if (e instanceof SQLTransactionRollbackException ||
        ((dbProduct == DatabaseProduct.MYSQL || dbProduct == DatabaseProduct.POSTGRES ||
          dbProduct == DatabaseProduct.SQLSERVER) && e.getSQLState().equals("40001")) ||
        (dbProduct == DatabaseProduct.POSTGRES && e.getSQLState().equals("40P01")) ||
        (dbProduct == DatabaseProduct.ORACLE && (e.getMessage().contains("deadlock detected")
          || e.getMessage().contains("can't serialize access for this transaction")))) {
        if (deadlockCnt++ < ALLOWED_REPEATED_DEADLOCKS) {
          long waitInterval = deadlockRetryInterval * deadlockCnt;
          LOG.warn("Deadlock detected in " + caller + ". Will wait " + waitInterval +
            "ms try again up to " + (ALLOWED_REPEATED_DEADLOCKS - deadlockCnt + 1) + " times.");
          // Pause for a just a bit for retrying to avoid immediately jumping back into the deadlock.
          try {
            Thread.sleep(waitInterval);
          } catch (InterruptedException ie) {
            // NOP
          }
          sendRetrySignal = true;
        } else {
          LOG.error("Too many repeated deadlocks in " + caller + ", giving up.");
        }
      } else if (isRetryable(e)) {
        //in MSSQL this means Communication Link Failure
        if (retryNum++ < retryLimit) {
          LOG.warn("Retryable error detected in " + caller + ".  Will wait " + retryInterval +
            "ms and retry up to " + (retryLimit - retryNum + 1) + " times.  Error: " + getMessage(e));
          try {
            Thread.sleep(retryInterval);
          } catch (InterruptedException ex) {
            //
          }
          sendRetrySignal = true;
        } else {
          LOG.error("Fatal error. Retry limit (" + retryLimit + ") reached. Last error: " + getMessage(e));
        }
      }
      else {
        //make sure we know we saw an error that we don't recognize
        LOG.info("Non-retryable error: " + getMessage(e));
      }
    }
    finally {
      /*if this method ends with anything except a retry signal, the caller should fail the operation
      and propagate the error up to the its caller (Metastore client); thus must reset retry counters*/
      if(!sendRetrySignal) {
        deadlockCnt = 0;
        retryNum = 0;
      }
    }
    if(sendRetrySignal) {
      throw new RetryException();
    }
  }

  /**
   * Determine the current time, using the RDBMS as a source of truth
   * @param conn database connection
   * @return current time in milliseconds
   * @throws org.apache.hadoop.hive.metastore.api.MetaException if the time cannot be determined
   */
  protected long getDbTime(Connection conn) throws MetaException {
    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      String s;
      switch (dbProduct) {
        case DERBY:
          s = "values current_timestamp";
          break;

        case MYSQL:
        case POSTGRES:
        case SQLSERVER:
          s = "select current_timestamp";
          break;

        case ORACLE:
          s = "select current_timestamp from dual";
          break;

        default:
          String msg = "Unknown database product: " + dbProduct.toString();
          LOG.error(msg);
          throw new MetaException(msg);
      }
      LOG.debug("Going to execute query <" + s + ">");
      ResultSet rs = stmt.executeQuery(s);
      if (!rs.next()) throw new MetaException("No results from date query");
      return rs.getTimestamp(1).getTime();
    } catch (SQLException e) {
      String msg = "Unable to determine current time: " + e.getMessage();
      LOG.error(msg);
      throw new MetaException(msg);
    } finally {
      closeStmt(stmt);
    }
  }

  /**
   * Determine the String that should be used to quote identifiers.
   * @param conn Active connection
   * @return quotes
   * @throws SQLException
   */
  protected String getIdentifierQuoteString(Connection conn) throws SQLException {
    if (identifierQuoteString == null) {
      identifierQuoteString = conn.getMetaData().getIdentifierQuoteString();
    }
    return identifierQuoteString;
  }

  protected enum DatabaseProduct { DERBY, MYSQL, POSTGRES, ORACLE, SQLSERVER}

  /**
   * Determine the database product type
   * @param conn database connection
   * @return database product type
   */
  private DatabaseProduct determineDatabaseProduct(Connection conn) {
    if (dbProduct == null) {
      try {
        String s = conn.getMetaData().getDatabaseProductName();
        if (s == null) {
          String msg = "getDatabaseProductName returns null, can't determine database product";
          LOG.error(msg);
          throw new IllegalStateException(msg);
        } else if (s.equals("Apache Derby")) {
          dbProduct = DatabaseProduct.DERBY;
        } else if (s.equals("Microsoft SQL Server")) {
          dbProduct = DatabaseProduct.SQLSERVER;
        } else if (s.equals("MySQL")) {
          dbProduct = DatabaseProduct.MYSQL;
        } else if (s.equals("Oracle")) {
          dbProduct = DatabaseProduct.ORACLE;
        } else if (s.equals("PostgreSQL")) {
          dbProduct = DatabaseProduct.POSTGRES;
        } else {
          String msg = "Unrecognized database product name <" + s + ">";
          LOG.error(msg);
          throw new IllegalStateException(msg);
        }

      } catch (SQLException e) {
        String msg = "Unable to get database product name: " + e.getMessage();
        LOG.error(msg);
        throw new IllegalStateException(msg);
      }
    }
    return dbProduct;
  }

  private static class LockInfo {
    private final long extLockId;
    private final long intLockId;
    //0 means there is no transaction, i.e. it a select statement which is not part of
    //explicit transaction or a IUD statement that is not writing to ACID table
    private final long txnId;
    private final String db;
    private final String table;
    private final String partition;
    private final LockState state;
    private final LockType type;

    // Assumes the result set is set to a valid row
    LockInfo(ResultSet rs) throws SQLException, MetaException {
      extLockId = rs.getLong("hl_lock_ext_id"); // can't be null
      intLockId = rs.getLong("hl_lock_int_id"); // can't be null
      db = rs.getString("hl_db"); // can't be null
      String t = rs.getString("hl_table");
      table = (rs.wasNull() ? null : t);
      String p = rs.getString("hl_partition");
      partition = (rs.wasNull() ? null : p);
      switch (rs.getString("hl_lock_state").charAt(0)) {
        case LOCK_WAITING: state = LockState.WAITING; break;
        case LOCK_ACQUIRED: state = LockState.ACQUIRED; break;
        default:
          throw new MetaException("Unknown lock state " + rs.getString("hl_lock_state").charAt(0));
      }
      switch (rs.getString("hl_lock_type").charAt(0)) {
        case LOCK_EXCLUSIVE: type = LockType.EXCLUSIVE; break;
        case LOCK_SHARED: type = LockType.SHARED_READ; break;
        case LOCK_SEMI_SHARED: type = LockType.SHARED_WRITE; break;
        default:
          throw new MetaException("Unknown lock type " + rs.getString("hl_lock_type").charAt(0));
      }
      txnId = rs.getLong("hl_txnid");//returns 0 if value is NULL
    }
    LockInfo(ShowLocksResponseElement e) {
      extLockId = e.getLockid();
      intLockId = e.getLockIdInternal();
      txnId = e.getTxnid();
      db = e.getDbname();
      table = e.getTablename();
      partition = e.getPartname();
      state = e.getState();
      type = e.getType();
    }

    public boolean equals(Object other) {
      if (!(other instanceof LockInfo)) return false;
      LockInfo o = (LockInfo)other;
      // Lock ids are unique across the system.
      return extLockId == o.extLockId && intLockId == o.intLockId;
    }

    @Override
    public String toString() {
      return JavaUtils.lockIdToString(extLockId) + " intLockId:" +
        intLockId + " " + JavaUtils.txnIdToString(txnId)
        + " db:" + db + " table:" + table + " partition:" +
        partition + " state:" + (state == null ? "null" : state.toString())
        + " type:" + (type == null ? "null" : type.toString());
    }
    private boolean isDbLock() {
      return db != null && table == null && partition == null;
    }
    private boolean isTableLock() {
      return db != null && table != null && partition == null;
    }
  }

  private static class LockInfoComparator implements Comparator<LockInfo> {
    private static final LockTypeComparator lockTypeComparator = new LockTypeComparator();
    public boolean equals(Object other) {
      return this == other;
    }

    public int compare(LockInfo info1, LockInfo info2) {
      // We sort by state (acquired vs waiting) and then by LockType, they by id
      if (info1.state == LockState.ACQUIRED &&
        info2.state != LockState .ACQUIRED) {
        return -1;
      }
      if (info1.state != LockState.ACQUIRED &&
        info2.state == LockState .ACQUIRED) {
        return 1;
      }

      int sortByType = lockTypeComparator.compare(info1.type, info2.type);
      if(sortByType != 0) {
        return sortByType;
      }
      if (info1.extLockId < info2.extLockId) {
        return -1;
      } else if (info1.extLockId > info2.extLockId) {
        return 1;
      } else {
        if (info1.intLockId < info2.intLockId) {
          return -1;
        } else if (info1.intLockId > info2.intLockId) {
          return 1;
        } else {
          return 0;
        }
      }
    }
  }

  /**
   * Sort more restrictive locks after less restrictive ones
   */
  private final static class LockTypeComparator implements Comparator<LockType> {
    public boolean equals(Object other) {
      return this == other;
    }
    public int compare(LockType t1, LockType t2) {
      switch (t1) {
        case EXCLUSIVE:
          if(t2 == LockType.EXCLUSIVE) {
            return 0;
          }
          return 1;
        case SHARED_WRITE:
          switch (t2) {
            case EXCLUSIVE:
              return -1;
            case SHARED_WRITE:
              return 0;
            case SHARED_READ:
              return 1;
            default:
              throw new RuntimeException("Unexpected LockType: " + t2);
          }
        case SHARED_READ:
          if(t2 == LockType.SHARED_READ) {
            return 0;
          }
          return -1;
        default:
          throw new RuntimeException("Unexpected LockType: " + t1);
      }
    }
  }
  private enum LockAction {ACQUIRE, WAIT, KEEP_LOOKING}

  // A jump table to figure out whether to wait, acquire,
  // or keep looking .  Since
  // java doesn't have function pointers (grumble grumble) we store a
  // character that we'll use to determine which function to call.
  // The table maps the lock type of the lock we are looking to acquire to
  // the lock type of the lock we are checking to the lock state of the lock
  // we are checking to the desired action.
  private static Map<LockType, Map<LockType, Map<LockState, LockAction>>> jumpTable;

  private void checkQFileTestHack() {
    boolean hackOn = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_IN_TEST) ||
      HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_IN_TEZ_TEST);
    if (hackOn) {
      LOG.info("Hacking in canned values for transaction manager");
      // Set up the transaction/locking db in the derby metastore
      TxnDbUtil.setConfValues(conf);
      try {
        TxnDbUtil.prepDb();
      } catch (Exception e) {
        // We may have already created the tables and thus don't need to redo it.
        if (e.getMessage() != null && !e.getMessage().contains("already exists")) {
          throw new RuntimeException("Unable to set up transaction database for" +
            " testing: " + e.getMessage(), e);
        }
      }
    }
  }

  private int abortTxns(Connection dbConn, List<Long> txnids) throws SQLException {
    return abortTxns(dbConn, txnids, -1);
  }
  /**
   * TODO: expose this as an operation to client.  Useful for streaming API to abort all remaining
   * trasnactions in a batch on IOExceptions.
   * Caller must rollback the transaction if not all transactions were aborted since this will not
   * attempt to delete associated locks in this case.
   *
   * @param dbConn An active connection
   * @param txnids list of transactions to abort
   * @param max_heartbeat value used by {@link #performTimeOuts()} to ensure this doesn't Abort txn which were
   *                      hearbetated after #performTimeOuts() select and this operation.
   * @return Number of aborted transactions
   * @throws SQLException
   */
  private int abortTxns(Connection dbConn, List<Long> txnids, long max_heartbeat) throws SQLException {
    Statement stmt = null;
    int updateCnt = 0;
    if (txnids.isEmpty()) {
      return 0;
    }
    try {
      stmt = dbConn.createStatement();
      //This is an update statement, thus at any Isolation level will take Write locks so will block
      //all other ops using S4U on TXNS row.
      List<String> queries = new ArrayList<String>();

      StringBuilder prefix = new StringBuilder();
      StringBuilder suffix = new StringBuilder();

      prefix.append("update TXNS set txn_state = " + quoteChar(TXN_ABORTED) +
        " where txn_state = " + quoteChar(TXN_OPEN) + " and ");
      if(max_heartbeat > 0) {
        suffix.append(" and txn_last_heartbeat < ").append(max_heartbeat);
      } else {
        suffix.append("");
      }

      TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, txnids, "txn_id", true, false);

      for (String query : queries) {
        LOG.debug("Going to execute update <" + query + ">");
        updateCnt = stmt.executeUpdate(query);
        if (updateCnt < txnids.size()) {
          /**
           * have to bail in this case since we don't know which transactions were not Aborted and
           * thus don't know which locks to delete
           * This may happen due to a race between {@link #heartbeat(HeartbeatRequest)}  operation and
           * {@link #performTimeOuts()}
           */
          return updateCnt;
        }
      }

      queries.clear();
      prefix.setLength(0);
      suffix.setLength(0);

      prefix.append("delete from HIVE_LOCKS where ");
      suffix.append("");

      TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, txnids, "hl_txnid", false, false);

      for (String query : queries) {
        LOG.debug("Going to execute update <" + query + ">");
        int rc = stmt.executeUpdate(query);
        LOG.debug("Removed " + rc + " records from HIVE_LOCKS");
      }
    } finally {
      closeStmt(stmt);
    }
    return updateCnt;
  }

  private static boolean isValidTxn(long txnId) {
    return txnId != 0;
  }
  /**
   * Note: this calls acquire() for (extLockId,intLockId) but extLockId is the same and we either take
   * all locks for given extLockId or none.  Would be more efficient to update state on all locks
   * at once.  Semantics are the same since this is all part of the same txn@serializable.
   *
   * Lock acquisition is meant to be fair, so every lock can only block on some lock with smaller
   * hl_lock_ext_id by only checking earlier locks.
   */
  private LockResponse checkLock(Connection dbConn,
                                 long extLockId)
    throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, MetaException, SQLException {
    if(dbConn.getTransactionIsolation() != Connection.TRANSACTION_SERIALIZABLE) {
      //longer term we should instead use AUX_TABLE/S4U to serialize all checkLock() operations
      //that would be less prone to deadlocks
      throw new IllegalStateException("Unexpected Isolation Level: " + dbConn.getTransactionIsolation());
    }
    List<LockInfo> locksBeingChecked = getLockInfoFromLockId(dbConn, extLockId);//being acquired now
    LockResponse response = new LockResponse();
    response.setLockid(extLockId);

    LOG.debug("checkLock(): Setting savepoint. extLockId=" + JavaUtils.lockIdToString(extLockId));
    Savepoint save = dbConn.setSavepoint();//todo: get rid of this
    StringBuilder query = new StringBuilder("select hl_lock_ext_id, " +
      "hl_lock_int_id, hl_db, hl_table, hl_partition, hl_lock_state, " +
      "hl_lock_type, hl_txnid from HIVE_LOCKS where hl_db in (");

    Set<String> strings = new HashSet<String>(locksBeingChecked.size());
    for (LockInfo info : locksBeingChecked) {
      strings.add(info.db);
    }
    boolean first = true;
    for (String s : strings) {
      if (first) first = false;
      else query.append(", ");
      query.append('\'');
      query.append(s);
      query.append('\'');
    }
    query.append(")");

    // If any of the table requests are null, then I need to pull all the
    // table locks for this db.
    boolean sawNull = false;
    strings.clear();
    for (LockInfo info : locksBeingChecked) {
      if (info.table == null) {
        sawNull = true;
        break;
      } else {
        strings.add(info.table);
      }
    }
    if (!sawNull) {
      query.append(" and (hl_table is null or hl_table in(");
      first = true;
      for (String s : strings) {
        if (first) first = false;
        else query.append(", ");
        query.append('\'');
        query.append(s);
        query.append('\'');
      }
      query.append("))");

      // If any of the partition requests are null, then I need to pull all
      // partition locks for this table.
      sawNull = false;
      strings.clear();
      for (LockInfo info : locksBeingChecked) {
        if (info.partition == null) {
          sawNull = true;
          break;
        } else {
          strings.add(info.partition);
        }
      }
      if (!sawNull) {
        query.append(" and (hl_partition is null or hl_partition in(");
        first = true;
        for (String s : strings) {
          if (first) first = false;
          else query.append(", ");
          query.append('\'');
          query.append(s);
          query.append('\'');
        }
        query.append("))");
      }
    }
    query.append(" and hl_lock_ext_id <= ").append(extLockId);

    LOG.debug("Going to execute query <" + query.toString() + ">");
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = dbConn.createStatement();
      rs = stmt.executeQuery(query.toString());
      SortedSet<LockInfo> lockSet = new TreeSet<LockInfo>(new LockInfoComparator());
      while (rs.next()) {
        lockSet.add(new LockInfo(rs));
      }
      // Turn the tree set into an array so we can move back and forth easily
      // in it.
      LockInfo[] locks = lockSet.toArray(new LockInfo[lockSet.size()]);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Locks to check(full): ");
        for(LockInfo info : locks) {
          LOG.debug("  " + info);
        }
      }

      for (LockInfo info : locksBeingChecked) {
        // Find the lock record we're checking
        int index = -1;
        for (int i = 0; i < locks.length; i++) {
          if (locks[i].equals(info)) {
            index = i;
            break;
          }
        }

        // If we didn't find the lock, then it must not be in the table
        if (index == -1) {
          LOG.debug("Going to rollback");
          dbConn.rollback();
          throw new MetaException("How did we get here, we heartbeated our lock before we started! ( " + info + ")");
        }


        // If we've found it and it's already been marked acquired,
        // then just look at the other locks.
        if (locks[index].state == LockState.ACQUIRED) {
          continue;
        }

        // Look at everything in front of this lock to see if it should block
        // it or not.
        boolean acquired = false;
        for (int i = index - 1; i >= 0; i--) {
          // Check if we're operating on the same database, if not, move on
          if (!locks[index].db.equals(locks[i].db)) {
            continue;
          }

          // If table is null on either of these, then they are claiming to
          // lock the whole database and we need to check it.  Otherwise,
          // check if they are operating on the same table, if not, move on.
          if (locks[index].table != null && locks[i].table != null
            && !locks[index].table.equals(locks[i].table)) {
            continue;
          }

          // If partition is null on either of these, then they are claiming to
          // lock the whole table and we need to check it.  Otherwise,
          // check if they are operating on the same partition, if not, move on.
          if (locks[index].partition != null && locks[i].partition != null
            && !locks[index].partition.equals(locks[i].partition)) {
            continue;
          }

          // We've found something that matches what we're trying to lock,
          // so figure out if we can lock it too.
          LockAction lockAction = jumpTable.get(locks[index].type).get(locks[i].type).get(locks[i].state);
          LOG.debug("desired Lock: " + info + " checked Lock: " + locks[i] + " action: " + lockAction);
          switch (lockAction) {
            case WAIT:
              if(!ignoreConflict(info, locks[i])) {
                /*we acquire all locks for a given query atomically; if 1 blocks, all go into (remain) in
                * Waiting state.  wait() will undo any 'acquire()' which may have happened as part of
                * this (metastore db) transaction and then we record which lock blocked the lock
                * we were testing ('info').*/
                wait(dbConn, save);
                String sqlText = "update HIVE_LOCKS" +
                  " set HL_BLOCKEDBY_EXT_ID=" + locks[i].extLockId +
                  ", HL_BLOCKEDBY_INT_ID=" + locks[i].intLockId +
                  " where HL_LOCK_EXT_ID=" + info.extLockId + " and HL_LOCK_INT_ID=" + info.intLockId;
                LOG.debug("Executing sql: " + sqlText);
                int updCnt = stmt.executeUpdate(sqlText);
                if(updCnt != 1) {
                  shouldNeverHappen(info.txnId, info.extLockId, info.intLockId);
                }
                LOG.debug("Going to commit");
                dbConn.commit();
                response.setState(LockState.WAITING);
                LOG.debug("Lock(" + info + ") waiting for Lock(" + locks[i] + ")");
                return response;
              }
              //fall through to ACQUIRE
            case ACQUIRE:
              acquire(dbConn, stmt, extLockId, info);
              acquired = true;
              break;
            case KEEP_LOOKING:
              continue;
          }
          if (acquired) break; // We've acquired this lock component,
          // so get out of the loop and look at the next component.
        }

        // If we've arrived here and we have not already acquired, it means there's nothing in the
        // way of the lock, so acquire the lock.
        if (!acquired) acquire(dbConn, stmt, extLockId, info);
      }

      // We acquired all of the locks, so commit and return acquired.
      LOG.debug("Going to commit");
      dbConn.commit();
      response.setState(LockState.ACQUIRED);
    } finally {
      close(rs, stmt, null);
    }
    return response;
  }

  /**
   * the {@link #jumpTable} only deals with LockState/LockType.  In some cases it's not
   * sufficient.  For example, an EXCLUSIVE lock on partition should prevent SHARED_READ
   * on the table, but there is no reason for EXCLUSIVE on a table to prevent SHARED_READ
   * on a database.
   */
  private boolean ignoreConflict(LockInfo desiredLock, LockInfo existingLock) {
    return
      ((desiredLock.isDbLock() && desiredLock.type == LockType.SHARED_READ &&
          existingLock.isTableLock() && existingLock.type == LockType.EXCLUSIVE) ||
        (existingLock.isDbLock() && existingLock.type == LockType.SHARED_READ &&
          desiredLock.isTableLock() && desiredLock.type == LockType.EXCLUSIVE))
        ||
      //different locks from same txn should not conflict with each other
      (desiredLock.txnId != 0 && desiredLock.txnId == existingLock.txnId) ||
      //txnId=0 means it's a select or IUD which does not write to ACID table, e.g
      //insert overwrite table T partition(p=1) select a,b from T and autoCommit=true
      (desiredLock.txnId == 0 &&  desiredLock.extLockId == existingLock.extLockId);
  }

  private void wait(Connection dbConn, Savepoint save) throws SQLException {
    // Need to rollback because we did a select that acquired locks but we didn't
    // actually update anything.  Also, we may have locked some locks as
    // acquired that we now want to not acquire.  It's ok to rollback because
    // once we see one wait, we're done, we won't look for more.
    // Only rollback to savepoint because we want to commit our heartbeat
    // changes.
    LOG.debug("Going to rollback to savepoint");
    dbConn.rollback(save);
  }

  private void acquire(Connection dbConn, Statement stmt, long extLockId, LockInfo lockInfo)
    throws SQLException, NoSuchLockException, MetaException {
    long now = getDbTime(dbConn);
    String s = "update HIVE_LOCKS set hl_lock_state = '" + LOCK_ACQUIRED + "', " +
      //if lock is part of txn, heartbeat info is in txn record
      "hl_last_heartbeat = " + (isValidTxn(lockInfo.txnId) ? 0 : now) +
    ", hl_acquired_at = " + now + " where hl_lock_ext_id = " +
      extLockId + " and hl_lock_int_id = " + lockInfo.intLockId;
    LOG.debug("Going to execute update <" + s + ">");
    int rc = stmt.executeUpdate(s);
    if (rc < 1) {
      LOG.debug("Going to rollback");
      dbConn.rollback();
      throw new NoSuchLockException("No such lock: (" + JavaUtils.lockIdToString(extLockId) + "," +
        + lockInfo.intLockId + ") " + JavaUtils.txnIdToString(lockInfo.txnId));
    }
    // We update the database, but we don't commit because there may be other
    // locks together with this, and we only want to acquire one if we can
    // acquire all.
  }

  /**
   * Heartbeats on the lock table.  This commits, so do not enter it with any state.
   * Should not be called on a lock that belongs to transaction.
   */
  private void heartbeatLock(Connection dbConn, long extLockId)
    throws NoSuchLockException, SQLException, MetaException {
    // If the lock id is 0, then there are no locks in this heartbeat
    if (extLockId == 0) return;
    Statement stmt = null;
    try {
      stmt = dbConn.createStatement();
      long now = getDbTime(dbConn);

      String s = "update HIVE_LOCKS set hl_last_heartbeat = " +
        now + " where hl_lock_ext_id = " + extLockId;
      LOG.debug("Going to execute update <" + s + ">");
      int rc = stmt.executeUpdate(s);
      if (rc < 1) {
        LOG.debug("Going to rollback");
        dbConn.rollback();
        throw new NoSuchLockException("No such lock: " + JavaUtils.lockIdToString(extLockId));
      }
      LOG.debug("Going to commit");
      dbConn.commit();
    } finally {
      closeStmt(stmt);
    }
  }

  // Heartbeats on the txn table.  This commits, so do not enter it with any state
  private void heartbeatTxn(Connection dbConn, long txnid)
    throws NoSuchTxnException, TxnAbortedException, SQLException, MetaException {
    // If the txnid is 0, then there are no transactions in this heartbeat
    if (txnid == 0) return;
    Statement stmt = null;
    try {
      stmt = dbConn.createStatement();
      long now = getDbTime(dbConn);
      String s = "update TXNS set txn_last_heartbeat = " + now +
        " where txn_id = " + txnid + " and txn_state = '" + TXN_OPEN + "'";
      LOG.debug("Going to execute update <" + s + ">");
      int rc = stmt.executeUpdate(s);
      if (rc < 1) {
        ensureValidTxn(dbConn, txnid, stmt); // This should now throw some useful exception.
        LOG.warn("Can neither heartbeat txn nor confirm it as invalid.");
        dbConn.rollback();
        throw new NoSuchTxnException("No such txn: " + txnid);
      }
      LOG.debug("Going to commit");
      dbConn.commit();
    } finally {
      closeStmt(stmt);
    }
  }

  private static void ensureValidTxn(Connection dbConn, long txnid, Statement stmt)
      throws SQLException, NoSuchTxnException, TxnAbortedException {
    // We need to check whether this transaction is valid and open
    String s = "select txn_state from TXNS where txn_id = " + txnid;
    LOG.debug("Going to execute query <" + s + ">");
    ResultSet rs = stmt.executeQuery(s);
    if (!rs.next()) {
      //todo: add LIMIT 1 instead of count - should be more efficient
      s = "select count(*) from COMPLETED_TXN_COMPONENTS where CTC_TXNID = " + txnid;
      ResultSet rs2 = stmt.executeQuery(s);
      boolean alreadyCommitted = rs2.next() && rs2.getInt(1) > 0;
      LOG.debug("Going to rollback");
      dbConn.rollback();
      if(alreadyCommitted) {
        //makes the message more informative - helps to find bugs in client code
        throw new NoSuchTxnException("Transaction " + JavaUtils.txnIdToString(txnid) + " is already committed.");
      }
      throw new NoSuchTxnException("No such transaction " + JavaUtils.txnIdToString(txnid));
    }
    if (rs.getString(1).charAt(0) == TXN_ABORTED) {
      LOG.debug("Going to rollback");
      dbConn.rollback();
      throw new TxnAbortedException("Transaction " + JavaUtils.txnIdToString(txnid) +
        " already aborted");//todo: add time of abort, which is not currently tracked.  Requires schema change
    }
  }

  private LockInfo getTxnIdFromLockId(Connection dbConn, long extLockId)
    throws NoSuchLockException, MetaException, SQLException {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = dbConn.createStatement();
      String s = "select hl_lock_ext_id, hl_lock_int_id, hl_db, hl_table, " +
        "hl_partition, hl_lock_state, hl_lock_type, hl_txnid from HIVE_LOCKS where " +
        "hl_lock_ext_id = " + extLockId;
      LOG.debug("Going to execute query <" + s + ">");
      rs = stmt.executeQuery(s);
      if (!rs.next()) {
        return null;
      }
      LockInfo info = new LockInfo(rs);
      LOG.debug("getTxnIdFromLockId(" + extLockId + ") Return " + JavaUtils.txnIdToString(info.txnId));
      return info;
    } finally {
      close(rs);
      closeStmt(stmt);
    }
  }

  // NEVER call this function without first calling heartbeat(long, long)
  private List<LockInfo> getLockInfoFromLockId(Connection dbConn, long extLockId)
    throws NoSuchLockException, MetaException, SQLException {
    Statement stmt = null;
    try {
      stmt = dbConn.createStatement();
      String s = "select hl_lock_ext_id, hl_lock_int_id, hl_db, hl_table, " +
        "hl_partition, hl_lock_state, hl_lock_type, hl_txnid from HIVE_LOCKS where " +
        "hl_lock_ext_id = " + extLockId;
      LOG.debug("Going to execute query <" + s + ">");
      ResultSet rs = stmt.executeQuery(s);
      boolean sawAtLeastOne = false;
      List<LockInfo> ourLockInfo = new ArrayList<LockInfo>();
      while (rs.next()) {
        ourLockInfo.add(new LockInfo(rs));
        sawAtLeastOne = true;
      }
      if (!sawAtLeastOne) {
        throw new MetaException("This should never happen!  We already " +
          "checked the lock(" + JavaUtils.lockIdToString(extLockId) + ") existed but now we can't find it!");
      }
      return ourLockInfo;
    } finally {
      closeStmt(stmt);
    }
  }

  // Clean time out locks from the database not associated with a transactions, i.e. locks
  // for read-only autoCommit=true statements.  This does a commit,
  // and thus should be done before any calls to heartbeat that will leave
  // open transactions.
  private void timeOutLocks(Connection dbConn, long now) {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = dbConn.createStatement();
      long maxHeartbeatTime = now - timeout;
      //doing a SELECT first is less efficient but makes it easier to debug things
      String s = "select distinct hl_lock_ext_id from HIVE_LOCKS where hl_last_heartbeat < " +
        maxHeartbeatTime + " and hl_txnid = 0";//when txnid is <> 0, the lock is
      //associated with a txn and is handled by performTimeOuts()
      //want to avoid expiring locks for a txn w/o expiring the txn itself
      List<Long> extLockIDs = new ArrayList<>();
      rs = stmt.executeQuery(s);
      while(rs.next()) {
        extLockIDs.add(rs.getLong(1));
      }
      rs.close();
      dbConn.commit();
      if(extLockIDs.size() <= 0) {
        return;
      }

      List<String> queries = new ArrayList<String>();

      StringBuilder prefix = new StringBuilder();
      StringBuilder suffix = new StringBuilder();

      //include same hl_last_heartbeat condition in case someone heartbeated since the select
      prefix.append("delete from HIVE_LOCKS where hl_last_heartbeat < ");
      prefix.append(maxHeartbeatTime);
      prefix.append(" and hl_txnid = 0 and ");
      suffix.append("");

      TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, extLockIDs, "hl_lock_ext_id", true, false);

      int deletedLocks = 0;
      for (String query : queries) {
        LOG.debug("Removing expired locks via: " + query);
        deletedLocks += stmt.executeUpdate(query);
      }
      if(deletedLocks > 0) {
        LOG.info("Deleted " + deletedLocks + " ext locks from HIVE_LOCKS due to timeout (vs. " +
            extLockIDs.size() + " found. List: " + extLockIDs + ") maxHeartbeatTime=" + maxHeartbeatTime);
      }
      LOG.debug("Going to commit");
      dbConn.commit();
    }
    catch(SQLException ex) {
      LOG.error("Failed to purge timedout locks due to: " + getMessage(ex), ex);
    }
    catch(Exception ex) {
      LOG.error("Failed to purge timedout locks due to: " + ex.getMessage(), ex);
    } finally {
      close(rs);
      closeStmt(stmt);
    }
  }

  /**
   * Suppose you have a query "select a,b from T" and you want to limit the result set
   * to the first 5 rows.  The mechanism to do that differs in different DB.
   * Make {@code noSelectsqlQuery} to be "a,b from T" and this method will return the
   * appropriately modified row limiting query.
   */
  private String addLimitClause(int numRows, String noSelectsqlQuery) throws MetaException {
    switch (dbProduct) {
      case DERBY:
        //http://db.apache.org/derby/docs/10.7/ref/rrefsqljoffsetfetch.html
        return "select " + noSelectsqlQuery + " fetch first " + numRows + " rows only";
      case MYSQL:
        //http://www.postgresql.org/docs/7.3/static/queries-limit.html
      case POSTGRES:
        //https://dev.mysql.com/doc/refman/5.0/en/select.html
        return "select " + noSelectsqlQuery + " limit " + numRows;
      case ORACLE:
        //newer versions (12c and later) support OFFSET/FETCH
        return "select * from (select " + noSelectsqlQuery + ") where rownum <= " + numRows;
      case SQLSERVER:
        //newer versions (2012 and later) support OFFSET/FETCH
        //https://msdn.microsoft.com/en-us/library/ms189463.aspx
        return "select TOP(" + numRows + ") " + noSelectsqlQuery;
      default:
        String msg = "Unrecognized database product name <" + dbProduct + ">";
        LOG.error(msg);
        throw new MetaException(msg);
    }
  }
  /**
   * Isolation Level Notes
   * Plain: RC is OK
   * This will find transactions that have timed out and abort them.
   * Will also delete locks which are not associated with a transaction and have timed out
   * Tries to keep transactions (against metastore db) small to reduce lock contention.
   */
  public void performTimeOuts() {
    Connection dbConn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
      //We currently commit after selecting the TXNS to abort.  So whether SERIALIZABLE
      //READ_COMMITTED, the effect is the same.  We could use FOR UPDATE on Select from TXNS
      //and do the whole performTimeOuts() in a single huge transaction, but the only benefit
      //would be to make sure someone cannot heartbeat one of these txns at the same time.
      //The attempt to heartbeat would block and fail immediately after it's unblocked.
      //With current (RC + multiple txns) implementation it is possible for someone to send
      //heartbeat at the very end of the expire interval, and just after the Select from TXNS
      //is made, in which case heartbeat will succeed but txn will still be Aborted.
      //Solving this corner case is not worth the perf penalty.  The client should heartbeat in a
      //timely way.
      long now = getDbTime(dbConn);
      timeOutLocks(dbConn, now);
      while(true) {
        stmt = dbConn.createStatement();
        String s = " txn_id from TXNS where txn_state = '" + TXN_OPEN +
          "' and txn_last_heartbeat <  " + (now - timeout);
        s = addLimitClause(250 * TIMED_OUT_TXN_ABORT_BATCH_SIZE, s);
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        if(!rs.next()) {
          return;//no more timedout txns
        }
        List<List<Long>> timedOutTxns = new ArrayList<>();
        List<Long> currentBatch = new ArrayList<>(TIMED_OUT_TXN_ABORT_BATCH_SIZE);
        timedOutTxns.add(currentBatch);
        do {
          currentBatch.add(rs.getLong(1));
          if(currentBatch.size() == TIMED_OUT_TXN_ABORT_BATCH_SIZE) {
            currentBatch = new ArrayList<>(TIMED_OUT_TXN_ABORT_BATCH_SIZE);
            timedOutTxns.add(currentBatch);
          }
        } while(rs.next());
        dbConn.commit();
        close(rs, stmt, null);
        int numTxnsAborted = 0;
        for(List<Long> batchToAbort : timedOutTxns) {
          if(abortTxns(dbConn, batchToAbort, now - timeout) == batchToAbort.size()) {
            dbConn.commit();
            numTxnsAborted += batchToAbort.size();
            //todo: add TXNS.COMMENT filed and set it to 'aborted by system due to timeout'
            LOG.info("Aborted the following transactions due to timeout: " + batchToAbort.toString());
          }
          else {
            //could not abort all txns in this batch - this may happen because in parallel with this
            //operation there was activity on one of the txns in this batch (commit/abort/heartbeat)
            //This is not likely but may happen if client experiences long pause between heartbeats or
            //unusually long/extreme pauses between heartbeat() calls and other logic in checkLock(),
            //lock(), etc.
            dbConn.rollback();
          }
        }
        LOG.info("Aborted " + numTxnsAborted + " transactions due to timeout");
      }
    } catch (SQLException ex) {
      LOG.warn("Aborting timedout transactions failed due to " + getMessage(ex), ex);
    }
    catch(MetaException e) {
      LOG.warn("Aborting timedout transactions failed due to " + e.getMessage(), e);
    }
    finally {
      close(rs, stmt, dbConn);
    }
  }

  private static synchronized void setupJdbcConnectionPool(HiveConf conf) throws SQLException {
    if (connPool != null) return;

    String driverUrl = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORECONNECTURLKEY);
    String user = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME);
    String passwd;
    try {
      passwd = ShimLoader.getHadoopShims().getPassword(conf,
        HiveConf.ConfVars.METASTOREPWD.varname);
    } catch (IOException err) {
      throw new SQLException("Error getting metastore password", err);
    }
    String connectionPooler = HiveConf.getVar(conf,
      HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE).toLowerCase();

    if ("bonecp".equals(connectionPooler)) {
      BoneCPConfig config = new BoneCPConfig();
      config.setJdbcUrl(driverUrl);
      //if we are waiting for connection for 60s, something is really wrong
      //better raise an error than hang forever
      config.setConnectionTimeoutInMs(60000);
      config.setMaxConnectionsPerPartition(10);
      config.setPartitionCount(1);
      config.setUser(user);
      config.setPassword(passwd);
      connPool = new BoneCPDataSource(config);
      doRetryOnConnPool = true;  // Enable retries to work around BONECP bug.
    } else if ("dbcp".equals(connectionPooler)) {
      ObjectPool objectPool = new GenericObjectPool();
      ConnectionFactory connFactory = new DriverManagerConnectionFactory(driverUrl, user, passwd);
      // This doesn't get used, but it's still necessary, see
      // http://svn.apache.org/viewvc/commons/proper/dbcp/branches/DBCP_1_4_x_BRANCH/doc/ManualPoolingDataSourceExample.java?view=markup
      PoolableConnectionFactory poolConnFactory =
        new PoolableConnectionFactory(connFactory, objectPool, null, null, false, true);
      connPool = new PoolingDataSource(objectPool);
    } else {
      throw new RuntimeException("Unknown JDBC connection pooling " + connectionPooler);
    }
  }

  private static synchronized void buildJumpTable() {
    if (jumpTable != null) return;

    jumpTable =
      new HashMap<LockType, Map<LockType, Map<LockState,  LockAction>>>(3);

    // SR: Lock we are trying to acquire is shared read
    Map<LockType, Map<LockState, LockAction>> m =
      new HashMap<LockType, Map<LockState, LockAction>>(3);
    jumpTable.put(LockType.SHARED_READ, m);

    // SR.SR: Lock we are examining is shared read
    Map<LockState, LockAction> m2 = new HashMap<LockState, LockAction>(2);
    m.put(LockType.SHARED_READ, m2);

    // SR.SR.acquired Lock we are examining is acquired;  We can acquire
    // because two shared reads can acquire together and there must be
    // nothing in front of this one to prevent acquisition.
    m2.put(LockState.ACQUIRED, LockAction.ACQUIRE);

    // SR.SR.wait Lock we are examining is waiting.  In this case we keep
    // looking, as it's possible that something in front is blocking it or
    // that the other locker hasn't checked yet and he could lock as well.
    m2.put(LockState.WAITING, LockAction.KEEP_LOOKING);

    // SR.SW: Lock we are examining is shared write
    m2 = new HashMap<LockState, LockAction>(2);
    m.put(LockType.SHARED_WRITE, m2);

    // SR.SW.acquired Lock we are examining is acquired;  We can acquire
    // because a read can share with a write, and there must be
    // nothing in front of this one to prevent acquisition.
    m2.put(LockState.ACQUIRED, LockAction.ACQUIRE);

    // SR.SW.wait Lock we are examining is waiting.  In this case we keep
    // looking, as it's possible that something in front is blocking it or
    // that the other locker hasn't checked yet and he could lock as well or
    // that something is blocking it that would not block a read.
    m2.put(LockState.WAITING, LockAction.KEEP_LOOKING);

    // SR.E: Lock we are examining is exclusive
    m2 = new HashMap<LockState, LockAction>(2);
    m.put(LockType.EXCLUSIVE, m2);

    // No matter whether it has acquired or not, we cannot pass an exclusive.
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);

    // SW: Lock we are trying to acquire is shared write
    m = new HashMap<LockType, Map<LockState, LockAction>>(3);
    jumpTable.put(LockType.SHARED_WRITE, m);

    // SW.SR: Lock we are examining is shared read
    m2 = new HashMap<LockState, LockAction>(2);
    m.put(LockType.SHARED_READ, m2);

    // SW.SR.acquired Lock we are examining is acquired;  We need to keep
    // looking, because there may or may not be another shared write in front
    // that would block us.
    m2.put(LockState.ACQUIRED, LockAction.KEEP_LOOKING);

    // SW.SR.wait Lock we are examining is waiting.  In this case we keep
    // looking, as it's possible that something in front is blocking it or
    // that the other locker hasn't checked yet and he could lock as well.
    m2.put(LockState.WAITING, LockAction.KEEP_LOOKING);

    // SW.SW: Lock we are examining is shared write
    m2 = new HashMap<LockState, LockAction>(2);
    m.put(LockType.SHARED_WRITE, m2);

    // Regardless of acquired or waiting, one shared write cannot pass another.
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);

    // SW.E: Lock we are examining is exclusive
    m2 = new HashMap<LockState, LockAction>(2);
    m.put(LockType.EXCLUSIVE, m2);

    // No matter whether it has acquired or not, we cannot pass an exclusive.
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);

    // E: Lock we are trying to acquire is exclusive
    m = new HashMap<LockType, Map<LockState, LockAction>>(3);
    jumpTable.put(LockType.EXCLUSIVE, m);

    // E.SR: Lock we are examining is shared read
    m2 = new HashMap<LockState, LockAction>(2);
    m.put(LockType.SHARED_READ, m2);

    // Exclusives can never pass
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);

    // E.SW: Lock we are examining is shared write
    m2 = new HashMap<LockState, LockAction>(2);
    m.put(LockType.SHARED_WRITE, m2);

    // Exclusives can never pass
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);

    // E.E: Lock we are examining is exclusive
    m2 = new HashMap<LockState, LockAction>(2);
    m.put(LockType.EXCLUSIVE, m2);

    // No matter whether it has acquired or not, we cannot pass an exclusive.
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);
  }
  /**
   * Returns true if {@code ex} should be retried
   */
  private static boolean isRetryable(Exception ex) {
    if(ex instanceof SQLException) {
      SQLException sqlException = (SQLException)ex;
      if("08S01".equalsIgnoreCase(sqlException.getSQLState())) {
        //in MSSQL this means Communication Link Failure
        return true;
      }
      if("ORA-08176".equalsIgnoreCase(sqlException.getSQLState()) ||
        sqlException.getMessage().contains("consistent read failure; rollback data not available")) {
        return true;
      }
      //see also https://issues.apache.org/jira/browse/HIVE-9938
    }
    return false;
  }
  private boolean isDuplicateKeyError(SQLException ex) {
    switch (dbProduct) {
      case DERBY:
        if("23505".equals(ex.getSQLState())) {
          return true;
        }
        break;
      case MYSQL:
        if(ex.getErrorCode() == 1022 && "23000".equals(ex.getSQLState())) {
          return true;
        }
        break;
      case SQLSERVER:
        //2627 is unique constaint violation incl PK, 2601 - unique key
        if(ex.getErrorCode() == 2627 && "23000".equals(ex.getSQLState())) {
          return true;
        }
        break;
      case ORACLE:
        if(ex.getErrorCode() == 1 && "23000".equals(ex.getSQLState())) {
          return true;
        }
        break;
      case POSTGRES:
        //http://www.postgresql.org/docs/8.1/static/errcodes-appendix.html
        if("23505".equals(ex.getSQLState())) {
          return true;
        }
        break;
      default:
        throw new IllegalArgumentException("Unexpected DB type: " + dbProduct + "; " + getMessage(ex));
    }
    return false;
  }
  private static String getMessage(SQLException ex) {
    return ex.getMessage() + "(SQLState=" + ex.getSQLState() + ",ErrorCode=" + ex.getErrorCode() + ")";
  }
  /**
   * Given a {@code selectStatement}, decorated it with FOR UPDATE or semantically equivalent
   * construct.  If the DB doesn't support, return original select.
   */
  private String addForUpdateClause(String selectStatement) throws MetaException {
    switch (dbProduct) {
      case DERBY:
        //https://db.apache.org/derby/docs/10.1/ref/rrefsqlj31783.html
        //sadly in Derby, FOR UPDATE doesn't meant what it should
        return selectStatement;
      case MYSQL:
        //http://dev.mysql.com/doc/refman/5.7/en/select.html
      case ORACLE:
        //https://docs.oracle.com/cd/E17952_01/refman-5.6-en/select.html
      case POSTGRES:
        //http://www.postgresql.org/docs/9.0/static/sql-select.html
        return selectStatement + " for update";
      case SQLSERVER:
        //https://msdn.microsoft.com/en-us/library/ms189499.aspx
        //https://msdn.microsoft.com/en-us/library/ms187373.aspx
        return selectStatement + " with(updlock)";
      default:
        String msg = "Unrecognized database product name <" + dbProduct + ">";
        LOG.error(msg);
        throw new MetaException(msg);
    }
  }
  static String quoteString(String input) {
    return "'" + input + "'";
  }
  static String quoteChar(char c) {
    return "'" + c + "'";
  }
  static CompactionType dbCompactionType2ThriftType(char dbValue) {
    switch (dbValue) {
      case MAJOR_TYPE:
        return CompactionType.MAJOR;
      case MINOR_TYPE:
        return CompactionType.MINOR;
      default:
        LOG.warn("Unexpected compaction type " + dbValue);
        return null;
    }
  }
  static Character thriftCompactionType2DbType(CompactionType ct) {
    switch (ct) {
      case MAJOR:
        return MAJOR_TYPE;
      case MINOR:
        return MINOR_TYPE;
      default:
        LOG.warn("Unexpected compaction type " + ct);
        return null;
    }
  }

  /**
   * {@link #lockInternal()} and {@link #unlockInternal()} are used to serialize those operations that require
   * Select ... For Update to sequence operations properly.  In practice that means when running
   * with Derby database.  See more notes at class level.
   */
  private void lockInternal() {
    if(dbProduct == DatabaseProduct.DERBY) {
      derbyLock.lock();
    }
  }
  private void unlockInternal() {
    if(dbProduct == DatabaseProduct.DERBY) {
      derbyLock.unlock();
    }
  }
  @Override
  public MutexAPI getMutexAPI() {
    return this;
  }

  @Override
  public LockHandle acquireLock(String key) throws MetaException {
    /**
     * The implementation here is a bit kludgey but done so that code exercised by unit tests
     * (which run against Derby which has no support for select for update) is as similar to
     * production code as possible.
     * In particular, with Derby we always run in a single process with a single metastore and
     * the absence of For Update is handled via a Semaphore.  The later would strictly speaking
     * make the SQL statments below unnecessary (for Derby), but then they would not be tested.
     */
    Connection dbConn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      try {
        String sqlStmt = addForUpdateClause("select MT_COMMENT from AUX_TABLE where MT_KEY1=" + quoteString(key) + " and MT_KEY2=0");
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        if(LOG.isDebugEnabled()) {
          LOG.debug("About to execute SQL: " + sqlStmt);
        }
        rs = stmt.executeQuery(sqlStmt);
        if (!rs.next()) {
          close(rs);
          try {
            stmt.executeUpdate("insert into AUX_TABLE(MT_KEY1,MT_KEY2) values(" + quoteString(key) + ", 0)");
            dbConn.commit();
          } catch (SQLException ex) {
            if (!isDuplicateKeyError(ex)) {
              throw new RuntimeException("Unable to lock " + quoteString(key) + " due to: " + getMessage(ex), ex);
            }
          }
          rs = stmt.executeQuery(sqlStmt);
          if (!rs.next()) {
            throw new IllegalStateException("Unable to lock " + quoteString(key) + ".  Expected row in AUX_TABLE is missing.");
          }
        }
        Semaphore derbySemaphore = null;
        if(dbProduct == DatabaseProduct.DERBY) {
          derbyKey2Lock.putIfAbsent(key, new Semaphore(1));
          derbySemaphore =  derbyKey2Lock.get(key);
          derbySemaphore.acquire();
        }
        LOG.info(quoteString(key) + " locked by " + quoteString(TxnHandler.hostname));
        //OK, so now we have a lock
        return new LockHandleImpl(dbConn, stmt, rs, key, derbySemaphore);
      } catch (SQLException ex) {
        rollbackDBConn(dbConn);
        close(rs, stmt, dbConn);
        checkRetryable(dbConn, ex, "acquireLock(" + key + ")");
        throw new MetaException("Unable to lock " + quoteString(key) + " due to: " + getMessage(ex) + "; " + StringUtils.stringifyException(ex));
      }
      catch(InterruptedException ex) {
        rollbackDBConn(dbConn);
        close(rs, stmt, dbConn);
        throw new MetaException("Unable to lock " + quoteString(key) + " due to: " + ex.getMessage() + StringUtils.stringifyException(ex));
      }
      finally {
        unlockInternal();
      }
    }
    catch(RetryException ex) {
      acquireLock(key);
    }
    throw new MetaException("This can't happen because checkRetryable() has a retry limit");
  }
  public void acquireLock(String key, LockHandle handle) {
    //the idea is that this will use LockHandle.dbConn
    throw new NotImplementedException();
  }
  private static final class LockHandleImpl implements LockHandle {
    private final Connection dbConn;
    private final Statement stmt;
    private final ResultSet rs;
    private final Semaphore derbySemaphore;
    private final List<String> keys = new ArrayList<>();
    LockHandleImpl(Connection conn, Statement stmt, ResultSet rs, String key, Semaphore derbySemaphore) {
      this.dbConn = conn;
      this.stmt = stmt;
      this.rs = rs;
      this.derbySemaphore = derbySemaphore;
      if(derbySemaphore != null) {
        //oterwise it may later release permit acquired by someone else
        assert derbySemaphore.availablePermits() == 0 : "Expected locked Semaphore";
      }
      keys.add(key);
    }
    void addKey(String key) {
      //keys.add(key);
      //would need a list of (stmt,rs) pairs - 1 for each key
      throw new NotImplementedException();
    }
    
    @Override
    public void releaseLocks() {
      rollbackDBConn(dbConn);
      close(rs, stmt, dbConn);
      if(derbySemaphore != null) {
        derbySemaphore.release();
      }
      for(String key : keys) {
        LOG.info(quoteString(key) + " unlocked by " + quoteString(TxnHandler.hostname));
      }
    }
  }
}
