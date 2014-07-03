package org.apache.hadoop.hive.ql.cube.parse;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.getDbName;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.getExpectedQuery;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.getWhereForDailyAndHourly2days;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.getWhereForDailyAndHourly2daysWithTimeDim;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.getWhereForHourly2days;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.getWhereForMonthly2months;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.getWhereForMonthlyDailyAndHourly2months;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.now;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.twoDaysRange;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.twoMonthsRangeUptoHours;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.twoMonthsRangeUptoMonth;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.twodaysBack;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.cube.metadata.StorageConstants;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCubeRewriter {

  private Configuration conf;
  private CubeQueryRewriter driver;
  private final String cubeName = CubeTestSetup.TEST_CUBE_NAME;

  static CubeTestSetup setup;
  static HiveConf hconf = new HiveConf(TestCubeRewriter.class);
  static String dbName;
  @BeforeClass
  public static void setup() throws Exception {
    SessionState.start(hconf);
    setup = new CubeTestSetup();
    String dbName = TestCubeRewriter.class.getSimpleName();
    setup.createSources(hconf, dbName);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    setup.dropSources(hconf);
  }

  @Before
  public void setupDriver() throws Exception {
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2");
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
  }

  @Test
  public void testQueryWithNow() throws Exception {
    SemanticException th = null;
    try {
      rewrite(driver, "select SUM(msr2) from testCube where" +
        " time_range_in('dt', 'NOW - 2DAYS', 'NOW')");
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.NO_CANDIDATE_FACT_AVAILABLE.getErrorCode());
  }

  @Test
  public void testCandidateTables() throws Exception {
    SemanticException th = null;
    try {
      rewrite(driver, "select dim12, SUM(msr2) from testCube" +
        " where " + twoDaysRange);
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.COLUMN_NOT_FOUND.getErrorCode());

    try {
      // this query should through exception because invalidMsr is invalid
      rewrite(driver, "SELECT cityid, invalidMsr from testCube " +
        " where " + twoDaysRange);
      Assert.assertTrue("Should not reach here", false);
    } catch (SemanticException exc) {
      exc.printStackTrace();
      Assert.assertNotNull(exc);
      th = exc;
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.COLUMN_NOT_FOUND.getErrorCode());
  }

  private CubeQueryContext rewrittenQuery;
  private String rewrite(CubeQueryRewriter driver, String query)
    throws SemanticException, ParseException {
    rewrittenQuery = driver.rewrite(query);
    return rewrittenQuery.toHQL();
  }

  @Test
  public void testCubeQuery() throws Exception {
    String hqlQuery = rewrite(driver, "cube select" +
      " SUM(msr2) from testCube where " + twoDaysRange);
    String expected = getExpectedQuery(cubeName,
      "select sum(testcube.msr2) FROM ", null, null,
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);
    System.out.println("Non existing parts:" + rewrittenQuery.getNonExistingParts());
    Assert.assertNotNull(rewrittenQuery.getNonExistingParts());

    // Query with column life not in the range
    SemanticException th = null;
    try {
      hqlQuery = rewrite(driver, "cube select SUM(newmeasure) from testCube" +
        " where " + twoDaysRange);
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.NOT_AVAILABLE_IN_RANGE.getErrorCode());
  }

  @Test
  public void testLightestFactFirst() throws Exception {
    // testFact is lighter than testFact2.
    String hqlQuery = rewrite(driver, "cube select" +
        " SUM(msr2) from testCube where " + twoDaysRange);
    String expected = getExpectedQuery(cubeName,
        "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
        " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
        null, null, getWhereForHourly2days("c1_testfact2"));
    compareQueries(expected, hqlQuery);

    conf.setBoolean(CubeQueryConfUtil.LIGHTEST_FACT_FIRST, true);
    conf.setBoolean(CubeQueryConfUtil.ADD_NON_EXISTING_PARTITIONS, true);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    SemanticException th = null;
    try {
      hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
          " where " + twoDaysRange);
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.NO_CANDIDATE_FACT_AVAILABLE.getErrorCode());
    Assert.assertTrue(th.getMessage()
        .contains("missingPartitions"));
  }

  @Test
  public void testDerivedCube() throws SemanticException, ParseException {
    String hqlQuery = rewrite(driver, "cube select" +
        " SUM(msr2) from derivedCube where " + twoDaysRange);
    String expected = getExpectedQuery(CubeTestSetup.DERIVED_CUBE_NAME,
        "select sum(derivedCube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(CubeTestSetup.DERIVED_CUBE_NAME, "C2_testfact"));
    compareQueries(expected, hqlQuery);
    System.out.println("Non existing parts:" + rewrittenQuery.getNonExistingParts());
    Assert.assertNotNull(rewrittenQuery.getNonExistingParts());

    SemanticException th = null;
    try {
      hqlQuery = rewrite(driver, "select SUM(msr4) from derivedCube" +
          " where " + twoDaysRange);
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.COLUMN_NOT_FOUND.getErrorCode());

    // test join
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "cube select" +
        " testdim2.name, SUM(msr2) from derivedCube where " + twoDaysRange);
    expected = getExpectedQuery(CubeTestSetup.DERIVED_CUBE_NAME,
        "select testdim2.name, sum(derivedCube.msr2) FROM ",
        " JOIN " + getDbName() + "c1_testdim2tbl testdim2 ON derivedCube.dim2 = " +
          " testdim2.id and (testdim2.dt = 'latest') ", null, "group by (testdim2.name)", null,
          getWhereForDailyAndHourly2days(CubeTestSetup.DERIVED_CUBE_NAME, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, true);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
  }

  @Test
  public void testCubeInsert() throws Exception {
    String hqlQuery = rewrite(driver, "insert overwrite directory" +
      " '/tmp/test' select SUM(msr2) from testCube where " + twoDaysRange);
    String expected = "insert overwrite directory '/tmp/test' " +
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "insert overwrite directory" +
        " '/tmp/test' cube select SUM(msr2) from testCube where " + twoDaysRange);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "insert overwrite local directory" +
      " '/tmp/test' select SUM(msr2) from testCube where " + twoDaysRange);
    expected = "insert overwrite local directory '/tmp/test' " +
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "insert overwrite local directory" +
        " '/tmp/test' cube select SUM(msr2) from testCube where " + twoDaysRange);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "insert overwrite table temp" +
      " select SUM(msr2) from testCube where " + twoDaysRange);
    expected = "insert overwrite table temp " +
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "insert overwrite table temp" +
        " cube select SUM(msr2) from testCube where " + twoDaysRange);
    compareQueries(expected, hqlQuery);

  }

  private void compareQueries(String expected, String actual) {
    if (expected == null && actual == null) {
      return;
    } else if (expected == null) {
      Assert.fail();
    } else if (actual == null) {
      Assert.fail("Rewritten query is null");
    }
    String expectedTrimmed = expected.replaceAll("\\W", "");
    String actualTrimmed = actual.replaceAll("\\W", "");

    if(!expectedTrimmed.equalsIgnoreCase(actualTrimmed)) {
      String method = null;
      for (StackTraceElement trace : Thread.currentThread().getStackTrace()) {
        if (trace.getMethodName().startsWith("test")) {
          method = trace.getMethodName() + ":" + trace.getLineNumber();
        }
      }

      System.err.println("__FAILED__ " + method
        + "\n\tExpected: " + expected + "\n\t---------\n\tActual: " + actual);
      System.err.println("\t__AGGR_EXPRS:" + rewrittenQuery.getAggregateExprs());
    }
    Assert.assertTrue(expectedTrimmed.equalsIgnoreCase(actualTrimmed));
  }

  @Test
  public void testCubeWhereQuery() throws Exception {
    String hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    String expected = getExpectedQuery(cubeName,
      "select sum(testcube.msr2) FROM ", null, null,
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    // Test with partition existence
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c1_testfact2"));
    compareQueries(expected, hqlQuery);
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);

    // Tests for valid tables
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact");
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact2");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c1_testfact2"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact2");
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testFact2"),
      "C1_testFact2");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c1_testfact2"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact");
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"),
      "C1_testFact");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact",
      "C1"), "HOURLY");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c1_testfact"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"),
      "C2_testFact");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C2"),
      "HOURLY");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c2_testfact"));
    compareQueries(expected, hqlQuery);

    // max interval test
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.QUERY_MAX_INTERVAL, "HOURLY");
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c2_testfact"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeWhereQueryWithMultipleTables() throws Exception {
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"),
      "C1_testFact,C2_testFact");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact",
      "C1"), "DAILY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact",
      "C2"), "HOURLY");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    String hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoDaysRange);

    String expected = null;
    if (!CubeTestSetup.isZerothHour()) {
      expected = getExpectedQuery(cubeName,
        "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "c1_testfact", "C2_testfact"));
    } else {
      expected = getExpectedQuery(cubeName,
        "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "c1_testfact"));
    }
    compareQueries(expected, hqlQuery);

    // Union query
    conf.setBoolean(CubeQueryConfUtil.ENABLE_MULTI_TABLE_SELECT, false);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    try {
      // rewrite to union query
      hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
          " where " + twoDaysRange);
      System.out.println("Union hql query:" + hqlQuery);

      //TODO: uncomment the following once union query
      // rewriting has been done
      // expected = // write expected union query
      // compareQueries(expected, hqlQuery);
    } catch (Exception e) {
      e.printStackTrace();
    }
    conf.setBoolean(CubeQueryConfUtil.ENABLE_MULTI_TABLE_SELECT, true);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"),
      "");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact",
      "C1"), "HOURLY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact",
      "C2"), "DAILY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact",
      "C3"), "MONTHLY");

    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoMonthsRangeUptoHours);
    if (!CubeTestSetup.isZerothHour()) {
      expected = getExpectedQuery(cubeName,
        "select sum(testcube.msr2) FROM ", null, null,
        getWhereForMonthlyDailyAndHourly2months("c1_testfact","C3_testfact",
          "c2_testFact"));
    } else {
      expected = getExpectedQuery(cubeName,
        "select sum(testcube.msr2) FROM ", null, null,
        getWhereForMonthlyDailyAndHourly2months("c1_testfact", "c2_testfact", "c3_testFact"));
    }
    compareQueries(expected, hqlQuery);

    // monthly - c1,c2; daily - c1, hourly -c2
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"),
      "C1_testFact,C2_testFact");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact",
      "C1"), "MONTHLY,DAILY");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact",
      "C2"), "MONTHLY,HOURLY");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    try {
      hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
          " where " + twoMonthsRangeUptoHours);
      System.out.println("union query:" + hqlQuery);
      //TODO: uncomment the following once union query
      // rewriting has been done
      //expected = getExpectedQuery(cubeName,
      //    "select sum(testcube.msr2) FROM ", null, null,
      //    getWhereForMonthlyDailyAndHourly2months("C1_testfact"));
      //compareQueries(expected, hqlQuery);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  @Test
  public void testCubeJoinQuery() throws Exception {
    // q1
    String hqlQuery = rewrite(driver, "select SUM(msr2) from testCube"
      + " join citydim on testCube.cityid = citydim.id"
      + " where " + twoDaysRange);
    List<String> joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt",
      "citydim", StorageConstants.getPartitionsForLatest()));
    String expected = getExpectedQuery(cubeName, "select sum(testcube.msr2)" +
      " FROM ", " INNER JOIN " + getDbName() + "c1_citytable citydim ON" +
      " testCube.cityid = citydim.id", null, null, joinWhereConds,
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube"
        + " join citydim on cityid = citydim.id"
        + " where " + twoDaysRange);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube"
        + " join citydim on cityid = id"
        + " where " + twoDaysRange);
    compareQueries(expected, hqlQuery);

    // q2
    hqlQuery = rewrite(driver, "select statedim.name, SUM(msr2) from"
      + " testCube"
      + " join citydim on testCube.cityid = citydim.id"
      + " left outer join statedim on statedim.id = citydim.stateid"
      + " right outer join zipdim on citydim.zipcode = zipdim.code"
      + " where " + twoDaysRange);
    joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt",
      "citydim", StorageConstants.getPartitionsForLatest()));
    joinWhereConds.add(StorageUtil.getWherePartClause("dt",
      "zipdim", StorageConstants.getPartitionsForLatest()));
    expected = getExpectedQuery(cubeName, "select statedim.name," +
      " sum(testcube.msr2) FROM ", "INNER JOIN " + getDbName() + "c1_citytable citydim ON" +
      " testCube.cityid = citydim.id LEFT OUTER JOIN " + getDbName() + "c1_statetable statedim"
      + " ON statedim.id = citydim.stateid AND " +
      "(statedim.dt = 'latest') RIGHT OUTER JOIN " + getDbName() + "c1_ziptable" +
      " zipdim ON citydim.zipcode = zipdim.code", null, " group by" +
      " statedim.name ", joinWhereConds,
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    // q3
    hqlQuery = rewrite(driver, "select st.name, SUM(msr2) from"
      + " testCube TC"
      + " join citydim CT on TC.cityid = CT.id"
      + " left outer join statedim ST on ST.id = CT.stateid"
      + " right outer join zipdim ZT on CT.zipcode = ZT.code"
      + " where " + twoDaysRange);
    joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt",
        "ct", StorageConstants.getPartitionsForLatest()));
    joinWhereConds.add(StorageUtil.getWherePartClause("dt",
      "zt", StorageConstants.getPartitionsForLatest()));
    expected = getExpectedQuery("tc", "select st.name," +
      " sum(tc.msr2) FROM ", " INNER JOIN " + getDbName() + "c1_citytable ct ON" +
      " tc.cityid = ct.id LEFT OUTER JOIN " + getDbName() + "c1_statetable st"
      + " ON st.id = ct.stateid and (st.dt = 'latest') " +
      "RIGHT OUTER JOIN " + getDbName() + "c1_ziptable" +
      " zt ON ct.zipcode = zt.code", null, " group by" +
      " st.name ", joinWhereConds,
      getWhereForDailyAndHourly2days("tc", "C2_testfact"));
    compareQueries(expected, hqlQuery);

    // q4
    hqlQuery = rewrite(driver, "select citydim.name, SUM(msr2) from"
      + " testCube"
      + " left outer join citydim on testCube.cityid = citydim.id"
      + " left outer join zipdim on citydim.zipcode = zipdim.code"
      + " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select citydim.name," +
      " sum(testcube.msr2) FROM ", " LEFT OUTER JOIN " + getDbName() + "c1_citytable citydim ON" +
      " testCube.cityid = citydim.id and (citydim.dt = 'latest') " +
      " LEFT OUTER JOIN " + getDbName() + "c1_ziptable" +
      " zipdim ON citydim.zipcode = zipdim.code AND " +
      "(zipdim.dt = 'latest')", null, " group by" +
      " citydim.name ", null,
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube"
      + " join countrydim on testCube.countryid = countrydim.id"
      + " where " + twoMonthsRangeUptoMonth);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      " INNER JOIN " + getDbName() + "c1_countrytable countrydim ON testCube.countryid = " +
        " countrydim.id", null, null, null,
      getWhereForMonthly2months("c2_testfactmonthly"));
    compareQueries(expected, hqlQuery);

    SemanticException th = null;
    try {
      hqlQuery = rewrite(driver, "select name, SUM(msr2) from testCube"
        + " join citydim" + " where " + twoDaysRange + " group by name");
      Assert.fail("Expected to throw exception");
    } catch (SemanticException e) {
      e.printStackTrace();
      th = e;
    }

    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.NO_JOIN_CONDITION_AVAIABLE.getErrorCode());
  }

  @Test
  public void testCubeGroupbyQuery() throws Exception {
    String hqlQuery = rewrite(driver, "select name, SUM(msr2) from" +
      " testCube join citydim on testCube.cityid = citydim.id where " +
      twoDaysRange);
    List<String> joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt",
      "citydim", StorageConstants.getPartitionsForLatest()));
    String expected = getExpectedQuery(cubeName, "select citydim.name," +
      " sum(testcube.msr2) FROM ", "INNER JOIN " + getDbName() + "c1_citytable citydim ON" +
      " testCube.cityid = citydim.id", null, " group by citydim.name ",
      joinWhereConds, getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube"
      + " join citydim on testCube.cityid = citydim.id"
      + " where " + twoDaysRange + " group by name");
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select cityid, SUM(msr2) from testCube"
      + " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select testcube.cityid," +
      " sum(testcube.msr2) FROM ", null, " group by testcube.cityid ",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select round(cityid), SUM(msr2) from" +
      " testCube where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select round(testcube.cityid)," +
      " sum(testcube.msr2) FROM ", null, " group by round(testcube.cityid) ",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube"
      + "  where " + twoDaysRange + "group by round(zipcode)");
    expected = getExpectedQuery(cubeName, "select round(testcube.zipcode)," +
      " sum(testcube.msr2) FROM ", null, " group by round(testcube.zipcode) ",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select round(cityid), SUM(msr2) from" +
      " testCube where " + twoDaysRange + " group by zipcode");
    expected = getExpectedQuery(cubeName, "select " +
      " round(testcube.cityid), sum(testcube.msr2) FROM ", null,
      " group by testcube.zipcode",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select round(cityid), SUM(msr2) from" +
        " testCube where " + twoDaysRange);
      expected = getExpectedQuery(cubeName, "select " +
        " round(testcube.cityid), sum(testcube.msr2) FROM ", null,
        " group by round(testcube.cityid)",
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
      compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select cityid, SUM(msr2) from testCube"
      + " where " + twoDaysRange + " group by round(zipcode)");
    expected = getExpectedQuery(cubeName, "select " +
      " testcube.cityid, sum(testcube.msr2) FROM ", null,
      " group by round(testcube.zipcode)",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube"
        + " where " + twoDaysRange + " group by round(zipcode)");
      expected = getExpectedQuery(cubeName, "select round(testcube.zipcode)," +
        " sum(testcube.msr2) FROM ", null,
        " group by round(testcube.zipcode)",
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
      compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select cityid, msr2 from testCube"
      + " where " + twoDaysRange + " group by round(zipcode)");
    expected = getExpectedQuery(cubeName, "select " +
      " testcube.cityid, sum(testcube.msr2) FROM ", null,
      " group by round(testcube.zipcode)",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select round(zipcode) rzc," +
      " msr2 from testCube where " + twoDaysRange + " group by zipcode" +
      " order by rzc");
    expected = getExpectedQuery(cubeName, "select round(testcube.zipcode) rzc,"
      + " sum(testcube.msr2) FROM ", null,
      " group by testcube.zipcode  order by rzc asc",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    // rewrite with expressions
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "SELECT citydim.name AS g1,"
       + " CASE  WHEN citydim.name=='NULL'  THEN 'NULL' "
       + " WHEN citydim.name=='X'  THEN 'X-NAME' "
       + " WHEN citydim.name=='Y'  THEN 'Y-NAME' "
       + " ELSE 'DEFAULT'   END  AS g2, "
       + " statedim.name AS g3,"
       + " statedim.id AS g4, "
       + " zipdim.code!=1  AND "
       + " ((zipdim.f1==\"xyz\"  AND  (zipdim.f2 >= \"3\"  AND "
       + "  zipdim.f2 !=\"NULL\"  AND  zipdim.f2 != \"uk\")) "
       + "  OR (zipdim.f2==\"adc\"  AND  zipdim.f1==\"js\" "
       + "  AND  ( citydim.name == \"X\"  OR  citydim.name == \"Y\" )) "
       + " OR ((zipdim.f1==\"api\"  OR  zipdim.f1==\"uk\"  OR  (zipdim.f1==\"adc\"  AND  zipdim.f1!=\"js\"))"
       + "  AND  citydim.id==12) ) AS g5,"
       + " zipdim.code==1  AND "
       + " ((zipdim.f1==\"xyz\"  AND  (zipdim.f2 >= \"3\"  AND "
       + "  zipdim.f2 !=\"NULL\"  AND  zipdim.f2 != \"uk\")) "
       + " OR (zipdim.f2==\"adc\"  AND  zipdim.f1==\"js\" "
       + " AND  ( citydim.name == \"X\"  OR  citydim.name == \"Y\" )) "
       + "  OR ((zipdim.f1==\"api\"  OR  zipdim.f1==\"uk\"  OR  (zipdim.f1==\"adc\"  AND  zipdim.f1!=\"js\"))"
       + "    AND  citydim.id==12) ) AS g6, "
       + "  zipdim.f1 AS g7, "
       + "  format_number(SUM(msr1),\"##################.###\") AS a1,"
       + "  format_number(SUM(msr2),\"##################.###\") AS a2, "
       + "  format_number(SUM(msr3),\"##################.###\") AS a3, "
       + " format_number(SUM(msr1)+SUM(msr2), \"##################.###\") AS a4,"
       + "  format_number(SUM(msr1)+SUM(msr3),\"##################.###\") AS a5,"
       + " format_number(SUM(msr1)-(SUM(msr2)+SUM(msr3)),\"##################.###\") AS a6"
       + "  FROM testCube where " + twoDaysRange + " HAVING (SUM(msr1) >=1000)  AND (SUM(msr2)>=0.01)");
    String actualExpr =  
        " join " + getDbName() + "c1_citytable citydim on testcube.cityid = citydim.id and (citydim.dt = 'latest')" +
        " join " + getDbName() + "c1_ziptable zipdim on testcube.zipcode = zipdim.code and (zipdim.dt = 'latest')  " +
        " join " + getDbName() + "c1_statetable statedim on testcube.stateid = statedim.id and (statedim.dt = 'latest')";
    expected = getExpectedQuery(cubeName, "SELECT ( citydim  .  name ) g1 ," +
    		"  case  when (( citydim  .  name ) ==  'NULL' ) then  'NULL'  when (( citydim  .  name ) ==  'X' )" +
    		" then  'X-NAME'  when (( citydim  .  name ) ==  'Y' ) then  'Y-NAME'" +
    		"  else  'DEFAULT'  end  g2 , ( statedim  .  name ) g3 , ( statedim  .  id ) g4 ," +
    		" ((( zipdim  .  code ) !=  1 ) and ((((( zipdim  .  f1 ) ==  \"xyz\" )" +
    		" and (((( zipdim  .  f2 ) >=  \"3\" ) and (( zipdim  .  f2 ) !=  \"NULL\" ))" +
    		" and (( zipdim  .  f2 ) !=  \"uk\" ))) or (((( zipdim  .  f2 ) ==  \"adc\" ) and (( zipdim  .  f1 ) ==  \"js\" ))" +
    		" and ((( citydim  .  name ) ==  \"X\" ) or (( citydim  .  name ) ==  \"Y\" )))) or ((((( zipdim  .  f1 ) ==  \"api\" )" +
    		" or (( zipdim  .  f1 ) ==  \"uk\" )) or ((( zipdim  .  f1 ) ==  \"adc\" ) and (( zipdim  .  f1 ) !=  \"js\" )))" +
    		" and (( citydim  .  id ) ==  12 )))) g5 , ((( zipdim  .  code ) ==  1 )" +
    		" and ((((( zipdim  .  f1 ) ==  \"xyz\" ) and (((( zipdim  .  f2 ) >=  \"3\" ) and (( zipdim  .  f2 ) !=  \"NULL\" ))" +
    		" and (( zipdim  .  f2 ) !=  \"uk\" ))) or (((( zipdim  .  f2 ) ==  \"adc\" ) and (( zipdim  .  f1 ) ==  \"js\" ))" +
    		" and ((( citydim  .  name ) ==  \"X\" ) or (( citydim  .  name ) ==  \"Y\" )))) or ((((( zipdim  .  f1 ) ==  \"api\" )" +
    		" or (( zipdim  .  f1 ) ==  \"uk\" )) or ((( zipdim  .  f1 ) ==  \"adc\" ) and (( zipdim  .  f1 ) !=  \"js\" )))" +
    		" and (( citydim  .  id ) ==  12 )))) g6 , ( zipdim  .  f1 ) g7 , format_number(sum(( testcube  .  msr1 )),  \"##################.###\" ) a1 ," +
    		" format_number(sum(( testcube  .  msr2 )),  \"##################.###\" ) a2 , format_number(sum(( testcube  .  msr3 )),  \"##################.###\" ) a3, " +
    		" format_number((sum(( testcube  .  msr1 )) + sum(( testcube  .  msr2 ))),  \"##################.###\" ) a4 ," +
    		" format_number((sum(( testcube  .  msr1 )) + sum(( testcube  .  msr3 ))),  \"##################.###\" ) a5 ," +
    		" format_number((sum(( testcube  .  msr1 )) - (sum(( testcube  .  msr2 )) + sum(( testcube  .  msr3 )))),  \"##################.###\" ) a6" +
    		"  FROM ",  actualExpr, null,
    	  " GROUP BY ( citydim  .  name ), case  when (( citydim  .  name ) ==  'NULL' ) " +
    	  "then  'NULL'  when (( citydim  .  name ) ==  'X' ) then  'X-NAME'  when (( citydim  .  name ) ==  'Y' )" +
    	  " then  'Y-NAME'  else  'DEFAULT'  end, ( statedim  .  name ), ( statedim  .  id )," +
    	  " ((( zipdim  .  code ) !=  1 ) and ((((( zipdim  .  f1 ) ==  \"xyz\" ) and (((( zipdim  .  f2 ) >=  \"3\" )" +
    	  " and (( zipdim  .  f2 ) !=  \"NULL\" )) and (( zipdim  .  f2 ) !=  \"uk\" ))) or (((( zipdim  .  f2 ) ==  \"adc\" )" +
    	  " and (( zipdim  .  f1 ) ==  \"js\" )) and ((( citydim  .  name ) ==  \"X\" ) or (( citydim  .  name ) ==  \"Y\" ))))" +
    	  " or ((((( zipdim  .  f1 ) ==  \"api\" ) or (( zipdim  .  f1 ) ==  \"uk\" )) or ((( zipdim  .  f1 ) ==  \"adc\" )" +
    	  " and (( zipdim  .  f1 ) !=  \"js\" ))) and (( citydim  .  id ) ==  12 )))), ((( zipdim  .  code ) ==  1 ) and" +
    	  " ((((( zipdim  .  f1 ) ==  \"xyz\" ) and (((( zipdim  .  f2 ) >=  \"3\" ) and (( zipdim  .  f2 ) !=  \"NULL\" ))" +
    	  " and (( zipdim  .  f2 ) !=  \"uk\" ))) or (((( zipdim  .  f2 ) ==  \"adc\" ) and (( zipdim  .  f1 ) ==  \"js\" ))" +
    	  " and ((( citydim  .  name ) ==  \"X\" ) or (( citydim  .  name ) ==  \"Y\" )))) or ((((( zipdim  .  f1 ) ==  \"api\" )" +
    	  " or (( zipdim  .  f1 ) ==  \"uk\" )) or ((( zipdim  .  f1 ) ==  \"adc\" ) and (( zipdim  .  f1 ) !=  \"js\" )))" +
    	  " and (( citydim  .  id ) ==  12 )))), ( zipdim  .  f1 ) HAVING ((sum(( testcube  .  msr1 )) >=  1000 ) and (sum(( testcube  .  msr2 )) >=  0.01 ))",
    	  null, getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
      compareQueries(expected, hqlQuery);

      hqlQuery = rewrite(driver, "SELECT citydim.name AS g1,"
          + " CASE  WHEN citydim.name=='NULL'  THEN 'NULL' "
          + " WHEN citydim.name=='X'  THEN 'X-NAME' "
          + " WHEN citydim.name=='Y'  THEN 'Y-NAME' "
          + " ELSE 'DEFAULT'   END  AS g2, "
          + " statedim.name AS g3,"
          + " statedim.id AS g4, "
          + " zipdim.code!=1  AND "
          + " ((zipdim.f1==\"xyz\"  AND  (zipdim.f2 >= \"3\"  AND "
          + "  zipdim.f2 !=\"NULL\"  AND  zipdim.f2 != \"uk\")) "
          + "  OR (zipdim.f2==\"adc\"  AND  zipdim.f1==\"js\" "
          + "  AND  ( citydim.name == \"X\"  OR  citydim.name == \"Y\" )) "
          + " OR ((zipdim.f1==\"api\"  OR  zipdim.f1==\"uk\"  OR  (zipdim.f1==\"adc\"  AND  zipdim.f1!=\"js\"))"
          + "  AND  citydim.id==12) ) AS g5,"
          + " zipdim.code==1  AND "
          + " ((zipdim.f1==\"xyz\"  AND  (zipdim.f2 >= \"3\"  AND "
          + "  zipdim.f2 !=\"NULL\"  AND  zipdim.f2 != \"uk\")) "
          + " OR (zipdim.f2==\"adc\"  AND  zipdim.f1==\"js\" "
          + " AND  ( citydim.name == \"X\"  OR  citydim.name == \"Y\" )) "
          + "  OR ((zipdim.f1==\"api\"  OR  zipdim.f1==\"uk\"  OR  (zipdim.f1==\"adc\"  AND  zipdim.f1!=\"js\"))"
          + "    AND  citydim.id==12) ) AS g6, "
          + "  zipdim.f1 AS g7, "
          + "  format_number(SUM(msr1),\"##################.###\") AS a1,"
          + "  format_number(SUM(msr2),\"##################.###\") AS a2, "
          + "  format_number(SUM(msr3),\"##################.###\") AS a3, "
          + " format_number(SUM(msr1)+SUM(msr2), \"##################.###\") AS a4,"
          + "  format_number(SUM(msr1)+SUM(msr3),\"##################.###\") AS a5,"
          + " format_number(SUM(msr1)-(SUM(msr2)+SUM(msr3)),\"##################.###\") AS a6"
          + "  FROM testCube where " + twoDaysRange
          + " group by citydim.name, CASE WHEN citydim.name=='NULL' THEN 'NULL'"
          + " WHEN citydim.name=='X' THEN 'X-NAME' WHEN citydim.name=='Y' THEN 'Y-NAME'"
          + " ELSE 'DEFAULT'   END, statedim.name, statedim.id,  zipdim.code!=1  AND"
          + "  ((zipdim.f1==\"xyz\"  AND  (zipdim.f2 >= \"3\"  AND zipdim.f2 !=\"NULL\"  AND  zipdim.f2 != \"uk\"))"
          + "   OR (zipdim.f2==\"adc\"  AND  zipdim.f1==\"js\" AND ( citydim.name == \"X\"  OR  citydim.name == \"Y\" ))"
          + "  OR ((zipdim.f1==\"api\"  OR  zipdim.f1==\"uk\"  OR  (zipdim.f1==\"adc\"  AND  zipdim.f1!=\"js\"))  AND  citydim.id==12) ),"
          + " zipdim.code==1  AND  ((zipdim.f1==\"xyz\" AND ( zipdim.f2 >= \"3\"  AND zipdim.f2 !=\"NULL\"  AND  zipdim.f2 != \"uk\"))"
          + "  OR (zipdim.f2==\"adc\"  AND  zipdim.f1==\"js\"  AND  ( citydim.name == \"X\"  OR  citydim.name == \"Y\" ))"
          + "   OR ((zipdim.f1=\"api\"  OR  zipdim.f1==\"uk\" OR (zipdim.f1==\"adc\"  AND  zipdim.f1!=\"js\")) AND  citydim.id==12)),"
          + " zipdim.f1 " + "HAVING (SUM(msr1) >=1000)  AND (SUM(msr2)>=0.01)");
      compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeQueryWithAilas() throws Exception {
    String hqlQuery = rewrite(driver, "select SUM(msr2) m2 from" +
      " testCube where " + twoDaysRange);
    String expected = getExpectedQuery(cubeName, "select sum(testcube.msr2)" +
      " m2 FROM ", null, null, getWhereForDailyAndHourly2days(cubeName,
      "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube mycube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery("mycube", "select sum(mycube.msr2) FROM ", null,
      null, getWhereForDailyAndHourly2days("mycube", "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select SUM(testCube.msr2) from testCube"
      + " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select mycube.msr2 m2 from testCube" +
      " mycube where " + twoDaysRange);
    expected = getExpectedQuery("mycube", "select sum(mycube.msr2) m2 FROM ",
      null, null, getWhereForDailyAndHourly2days("mycube", "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select testCube.msr2 m2 from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) m2 FROM ",
      null, null, getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeWhereQueryForMonth() throws Exception {
    String hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoMonthsRangeUptoHours);
    String expected = getExpectedQuery(cubeName,
      "select sum(testcube.msr2) FROM ", null, null,
      getWhereForMonthlyDailyAndHourly2months("C2_testfact"));
    compareQueries(expected, hqlQuery);

    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    SemanticException th = null;
    try {
      hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
        " where " + twoMonthsRangeUptoHours);
      Assert.assertTrue(false);
    } catch (SemanticException e) {
      e.printStackTrace();
      th = e;
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.NO_CANDIDATE_FACT_AVAILABLE.getErrorCode());
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));

    // this should consider only two month partitions.
    hqlQuery = rewrite(driver, "select cityid, SUM(msr2) from testCube"
      + " where " + twoMonthsRangeUptoMonth);
    expected = getExpectedQuery(cubeName, "select testcube.cityid," +
      " sum(testcube.msr2) FROM ", null, "group by testcube.cityid",
      getWhereForMonthly2months("c2_testfact"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testDimensionQueryWithMultipleStorages() throws Exception {
    String hqlQuery = rewrite(driver, "select name, stateid from" +
      " citydim");
    String expected = getExpectedQuery("citydim", "select citydim.name," +
      " citydim.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    // should pick up c2 storage when 'fail on partial data' enabled
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select name, stateid from" +
        " citydim");
    expected = getExpectedQuery("citydim", "select citydim.name," +
        " citydim.stateid from ", null, "c2_citytable", false);
      compareQueries(expected, hqlQuery);

    // state table is present on c1 with partition dumps and partitions added
    SemanticException th = null;
    try {
      hqlQuery = rewrite(driver, "select name, capital from statedim ");
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.NO_CANDIDATE_DIM_AVAILABLE.getErrorCode());
    Assert.assertTrue(th.getMessage().contains("statedim"));

    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));

    // non existing parts should be populated
    hqlQuery = rewrite(driver, "select name, capital from statedim ");
    expected = getExpectedQuery("statedim", "select statedim.name," +
        " statedim.capital from ", null, "c1_statetable", true);
    compareQueries(expected, hqlQuery);
    Assert.assertNotNull(rewrittenQuery.getNonExistingParts());

    // run a query with time range function
    hqlQuery = rewrite(driver, "select name, stateid from citydim where " + twoDaysRange);
    expected = getExpectedQuery("citydim", "select citydim.name," +
        " citydim.stateid from ", twoDaysRange,  null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    // query with alias
    hqlQuery = rewrite(driver, "select name, c.stateid from citydim" +
      " c");
    expected = getExpectedQuery("c", "select c.name, c.stateid from ", null,
      "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    // query with where clause
    hqlQuery = rewrite(driver, "select name, c.stateid from citydim" +
        " c where name != 'xyz' ");
    expected = getExpectedQuery("c", "select c.name, c.stateid from ",
        " c.name != 'xyz' ", null,
        "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    // query with orderby
    hqlQuery = rewrite(driver, "select name, c.stateid from citydim" +
        " c where name != 'xyz' order by name");
    expected = getExpectedQuery("c", "select c.name, c.stateid from ",
        " c.name != 'xyz' ", " order by c.name asc",
        "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    // query with where and orderby
    hqlQuery = rewrite(driver, "select name, c.stateid from citydim" +
        " c where name != 'xyz' order by name");
    expected = getExpectedQuery("c", "select c.name, c.stateid from ",
        " c.name != 'xyz' ", " order by c.name asc ",
        "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    // query with orderby with order specified
    hqlQuery = rewrite(driver, "select name, c.stateid from citydim" +
        " c where name != 'xyz' order by name desc ");
    expected = getExpectedQuery("c", "select c.name, c.stateid from ",
        " c.name != 'xyz' ", " order by c.name desc",
        "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select name, stateid from citydim");
    expected = getExpectedQuery("citydim", "select citydim.name," +
      " citydim.stateid from ", null, "c2_citytable", false);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select name, stateid from citydim");
    expected = getExpectedQuery("citydim", "select citydim.name," +
      " citydim.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES, "C1_citytable");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select name, stateid from citydim");
    expected = getExpectedQuery("citydim", "select citydim.name," +
      " citydim.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES, "C2_citytable");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select name, stateid from citydim");
    expected = getExpectedQuery("citydim", "select citydim.name," +
      " citydim.stateid from ", null, "c2_citytable", false);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select name n, count(1) from citydim"
      + " group by name order by n ");
    expected = getExpectedQuery("citydim", "select citydim.name n," +
      " count(1) from ", "groupby citydim.name order by n asc", "c2_citytable",
      false);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select name n, count(1) from citydim"
      + " order by n ");
    compareQueries(expected, hqlQuery);
    hqlQuery = rewrite(driver, "select count(1) from citydim"
      + " group by name order by name ");
    expected = getExpectedQuery("citydim", "select citydim.name," +
      " count(1) from ", "groupby citydim.name order by citydim.name asc ",
      "c2_citytable", false);
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testLimitQueryOnDimension() throws Exception {
    String hqlQuery = rewrite(driver, "select name, stateid from" +
      " citydim limit 100");
    String expected = getExpectedQuery("citydim", "select citydim.name," +
      " citydim.stateid from ", " limit 100", "c1_citytable", true);
    compareQueries(expected, hqlQuery);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select name, stateid from citydim " +
      "limit 100");
    expected = getExpectedQuery("citydim", "select citydim.name," +
      "citydim.stateid from ", " limit 100", "c2_citytable", false);
    compareQueries(expected, hqlQuery);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select name, stateid from citydim" +
      " limit 100");
    expected = getExpectedQuery("citydim", "select citydim.name," +
      " citydim.stateid from ", " limit 100", "c1_citytable", true);
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testAggregateResolver() throws Exception {
    // pass
    String q1 = "SELECT cityid, testCube.msr2 from testCube where "
      + twoDaysRange;

    // pass
    String q2 = "SELECT cityid, testCube.msr2 * testCube.msr3 from testCube where "
      + twoDaysRange;

    // pass
    String q3 = "SELECT cityid, sum(testCube.msr2) from testCube where "
      + twoDaysRange;

    // pass
    String q4 = "SELECT cityid, sum(testCube.msr2) from testCube where "
      + twoDaysRange + " having testCube.msr2 > 100";

    // pass
    String q5 = "SELECT cityid, testCube.msr2 from testCube where "
      + twoDaysRange + " having testCube.msr2 + testCube.msr3 > 100";

    // pass
    String q6 = "SELECT cityid, testCube.msr2 from testCube where "
      + twoDaysRange + " having testCube.msr2 > 100 AND testCube.msr2 < 1000";

    // pass
    String q7 = "SELECT cityid, sum(testCube.msr2) from testCube where "
      + twoDaysRange + " having (testCube.msr2 > 100) OR (testcube.msr2 < 100" +
      " AND SUM(testcube.msr3) > 1000)";

    // pass
    String q8 = "SELECT cityid, sum(testCube.msr2) * sum(testCube.msr3) from" +
      " testCube where " + twoDaysRange;

    // pass
    String q9 = "SELECT cityid c1, sum(msr3) m3 from testCube where "
      + "c1 > 100 and " + twoDaysRange + " having (msr2 < 100" +
      " AND m3 > 1000)";

    String expectedq1 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) from ", null, "group by testcube.cityid",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq2 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
        " sum(testCube.msr2) * max(testCube.msr3) from ", null,
        "group by testcube.cityid",
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq3 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) from ", null, "group by testcube.cityid",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq4 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) from ", null, "group by testcube.cityid having" +
      " sum(testCube.msr2) > 100",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq5 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) from ", null, "group by testcube.cityid having" +
      " sum(testCube.msr2) + max(testCube.msr3) > 100",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq6 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) from ", null, "group by testcube.cityid having" +
      " sum(testCube.msr2) > 100 and sum(testCube.msr2) < 1000",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq7 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) from ", null, "group by testcube.cityid having" +
      " sum(testCube.msr2) > 100) OR (sum(testCube.msr2) < 100 AND" +
      " SUM(testcube.msr3) > 1000)",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq8 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) * sum(testCube.msr3) from ", null,
      "group by testcube.cityid",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq9 = getExpectedQuery(cubeName, "SELECT testcube.cityid c1,"
      + " sum(testCube.msr3) m3 from ", "c1 > 100", "group by testcube.cityid" +
      " having sum(testCube.msr2) < 100 AND (m3 > 1000)",
      getWhereForDailyAndHourly2days(cubeName, "c2_testfact"));

    String tests[] = {q1, q2, q3, q4, q5, q6, q7, q8, q9};
    String expected[] = {expectedq1, expectedq2, expectedq3, expectedq4,
      expectedq5, expectedq6, expectedq7, expectedq8, expectedq9};

    for (int i = 0; i < tests.length; i++) {
      String hql  = rewrite(driver, tests[i]);
      compareQueries(expected[i], hql);
    }
    String failq = "SELECT cityid, testCube.noAggrMsr FROM testCube where "
      + twoDaysRange;
    SemanticException th = null;
    try {
      // Should throw exception in aggregate resolver because noAggrMsr does
      //not have a default aggregate defined.
      String hql = rewrite(driver, failq);
      Assert.assertTrue("Should not reach here: " + hql, false);
    } catch (SemanticException exc) {
      exc.printStackTrace();
      th = exc;
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.NO_DEFAULT_AGGREGATE.getErrorCode());
    String queryWithFunction = "SELECT cityid, exp(testCube.msr2) FROM testCube where " + twoDaysRange;
    String hql = rewrite(driver, queryWithFunction);
    System.out.println("#### AGG resolver HQL with function " + hql);

    String expectedHqlWithFunction = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
        " exp(sum(testCube.msr2)) from ", null,
        "group by testcube.cityid",
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expectedHqlWithFunction, hql);

    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, true);

    // Test if raw fact is selected for query with no aggregate function on a measure, with aggregate resolver disabled
    String query = "SELECT cityid, testCube.msr2 FROM testCube WHERE " + twoDaysRange;
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, true);
    CubeQueryRewriter driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    CubeQueryContext cubeql = driver.rewrite(query);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    CubeQueryContext.CandidateFact candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());

    // Check a query with non default aggregate function
    String nonDefaultAggrQuery = "SELECT cityid, avg(testCube.msr2) FROM testCube WHERE " + twoDaysRange;
    cubeql = driver.rewrite(nonDefaultAggrQuery);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());

    // query with measure in a where clause
    query = "SELECT cityid, sum(testCube.msr2) FROM testCube WHERE testCube.msr1 < 100 and " + twoDaysRange;
    cubeql = driver.rewrite(query);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());

    query = "SELECT cityid, sum(testCube.msr2) FROM testCube WHERE " + twoDaysRange + " group by testCube.msr1";
    cubeql = driver.rewrite(query);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());

    query = "SELECT cityid, sum(testCube.msr2) FROM testCube WHERE " + twoDaysRange + " order by testCube.msr1";
    cubeql = driver.rewrite(query);
    Assert.assertEquals(1, cubeql.getCandidateFactTables().size());
    candidateFact = cubeql.getCandidateFactTables().iterator().next();
    Assert.assertEquals("testFact2_raw".toLowerCase(), candidateFact.fact.getName().toLowerCase());
  }

  @Test
  public void testColumnAmbiguity() throws Exception {
    String query = "SELECT ambigdim1, sum(testCube.msr1) FROM testCube join" +
      " citydim on testcube.cityid = citydim.id where " + twoDaysRange;

    SemanticException th = null;
    try {
      String hql = rewrite(driver, query);
      Assert.assertTrue("Should not reach here:" + hql, false);
    } catch (SemanticException exc) {
      exc.printStackTrace();
      th = exc;
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.AMBIGOUS_CUBE_COLUMN.getErrorCode());

    th = null;
    String q2 = "SELECT ambigdim2 from citydim join" +
      " statedim on citydim.stateid = statedim.id join countrydim on" +
      " statedim.countryid = countrydim.id";
    try {
      String hql = rewrite(driver, q2);
      Assert.fail("Should not reach here: " + hql);
    } catch (SemanticException exc) {
      exc.printStackTrace();
      th = exc;
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.AMBIGOUS_DIM_COLUMN.getErrorCode());
  }

  @Test
  public void testAliasReplacer() throws Exception {
    String queries[] = {
      "SELECT cityid, t.msr2 FROM testCube t where " + twoDaysRange,
      "SELECT cityid, msr2 FROM testCube where msr2 > 100 and " + twoDaysRange +
        " HAVING msr2 < 1000",
      "SELECT cityid, testCube.msr2 FROM testCube where msr2 > 100 and "
        + twoDaysRange + " HAVING msr2 < 1000 ORDER BY cityid"
    };

    String expectedQueries[] = {
      getExpectedQuery("t", "SELECT t.cityid, sum(t.msr2) FROM ", null,
        " group by t.cityid", getWhereForDailyAndHourly2days("t",
        "C2_testfact")),
      getExpectedQuery(cubeName, "SELECT testCube.cityid, sum(testCube.msr2)" +
        " FROM ", " testcube.msr2 > 100 ", " group by testcube.cityid having" +
        " sum(testCube.msr2 < 1000)", getWhereForDailyAndHourly2days(
        cubeName, "C2_testfact")),
      getExpectedQuery(cubeName, "SELECT testCube.cityid, sum(testCube.msr2)" +
        " FROM ", " testcube.msr2 > 100 ", " group by testcube.cityid having" +
        " sum(testCube.msr2 < 1000) orderby testCube.cityid asc",
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact")),
    };

    for (int i = 0; i < queries.length; i++) {
      String hql = rewrite(driver, queries[i]);
      compareQueries(expectedQueries[i], hql);
    }
  }

  @Test
  public void testFactsWithInvalidColumns() throws Exception {
    String hqlQuery = rewrite(driver, "select dim1, AVG(msr1)," +
      " msr2 from testCube" +
      " where " + twoDaysRange);
    String expected = getExpectedQuery(cubeName,
      "select testcube.dim1, avg(testcube.msr1), sum(testcube.msr2) FROM ",
      null, " group by testcube.dim1",
      getWhereForDailyAndHourly2days(cubeName, "C1_summary1"));
    compareQueries(expected, hqlQuery);
    hqlQuery = rewrite(driver, "select dim1, dim2, COUNT(msr1)," +
      " SUM(msr2), msr3 from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName,
      "select testcube.dim1, testcube,dim2, count(testcube.msr1)," +
        " sum(testcube.msr2), max(testcube.msr3) FROM ", null,
      " group by testcube.dim1, testcube.dim2",
      getWhereForDailyAndHourly2days(cubeName, "C1_summary2"));
    compareQueries(expected, hqlQuery);
    hqlQuery = rewrite(driver, "select dim1, dim2, cityid, SUM(msr1)," +
      " SUM(msr2), msr3 from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName,
      "select testcube.dim1, testcube,dim2, testcube.cityid," +
        " sum(testcube.msr1), sum(testcube.msr2), max(testcube.msr3) FROM ",
      null, " group by testcube.dim1, testcube.dim2, testcube.cityid",
      getWhereForDailyAndHourly2days(cubeName, "C1_summary3"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testFactsWithTimedDimension() throws Exception {
    String twoDaysITRange = "time_range_in('it', '" +
      CubeTestSetup.getDateUptoHours(
      twodaysBack) + "','" + CubeTestSetup.getDateUptoHours(now) + "')";

    String hqlQuery = rewrite(driver, "select dim1, AVG(msr1)," +
      " msr2 from testCube" +
      " where " + twoDaysITRange);
    String expected = getExpectedQuery(cubeName,
      "select testcube.dim1, avg(testcube.msr1), sum(testcube.msr2) FROM ",
      null, " group by testcube.dim1",
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary1"));
    compareQueries(expected, hqlQuery);
    hqlQuery = rewrite(driver, "select dim1, dim2, COUNT(msr1)," +
      " SUM(msr2), msr3 from testCube" +
      " where " + twoDaysITRange);
    expected = getExpectedQuery(cubeName,
      "select testcube.dim1, testcube,dim2, count(testcube.msr1)," +
        " sum(testcube.msr2), max(testcube.msr3) FROM ", null,
      " group by testcube.dim1, testcube.dim2",
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary2"));
    compareQueries(expected, hqlQuery);
    hqlQuery = rewrite(driver, "select dim1, dim2, cityid, SUM(msr1)," +
      " SUM(msr2), msr3 from testCube" +
      " where " + twoDaysITRange);
    expected = getExpectedQuery(cubeName,
      "select testcube.dim1, testcube,dim2, testcube.cityid," +
        " sum(testcube.msr1), sum(testcube.msr2), max(testcube.msr3) FROM ",
      null, " group by testcube.dim1, testcube.dim2, testcube.cityid",
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary3"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeQueryTimedDimensionFilter() throws Exception {
    String twoDaysITRange = "time_range_in('it', '" +
      CubeTestSetup.getDateUptoHours(
      twodaysBack) + "','" + CubeTestSetup.getDateUptoHours(now) + "')";

    String hqlQuery = rewrite(driver, "select dim1, AVG(msr1)," +
      " msr2 from testCube" +
      " where (" + twoDaysITRange + " OR it == 'default') AND dim1 > 1000");
    String expected = getExpectedQuery(cubeName,
      "select testcube.dim1, avg(testcube.msr1), sum(testcube.msr2) FROM ",
      null, "or (( testcube.it ) == 'default')) and ((testcube.dim1) > 1000)"
      + " group by testcube.dim1",
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "it", "C2_summary1"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoDaysRange + " OR (" + CubeTestSetup.twoDaysRangeBefore4days + " AND dt='default')");

    String expecteddtRangeWhere1 = getWhereForDailyAndHourly2daysWithTimeDim(
      cubeName, "dt", twodaysBack, now) + " OR (" +
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt",
          CubeTestSetup.before4daysStart, CubeTestSetup.before4daysEnd) + ")";
    expected = getExpectedQuery(cubeName,
      "select sum(testcube.msr2) FROM ", null, " AND testcube.dt='default'",
      expecteddtRangeWhere1, "c2_testfact");
    compareQueries(expected, hqlQuery);

    String expecteddtRangeWhere2 = "(" + getWhereForDailyAndHourly2daysWithTimeDim(
      cubeName, "dt", twodaysBack, now) + " AND testcube.dt='dt1') OR " +
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt",
          CubeTestSetup.before4daysStart, CubeTestSetup.before4daysEnd);
    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where (" + twoDaysRange + " AND dt='dt1') OR (" +
      CubeTestSetup.twoDaysRangeBefore4days + " AND dt='default')");
    expected = getExpectedQuery(cubeName,
      "select sum(testcube.msr2) FROM ", null, " AND testcube.dt='default'",
    expecteddtRangeWhere2, "c2_testfact");
    compareQueries(expected, hqlQuery);

    String twoDaysPTRange = "time_range_in('pt', '" +
        CubeTestSetup.getDateUptoHours(
        twodaysBack) + "','" + CubeTestSetup.getDateUptoHours(now) + "')";
    hqlQuery = rewrite(driver, "select dim1, AVG(msr1)," +
      " msr2 from testCube where (" + twoDaysITRange +
      " OR (" + twoDaysPTRange + " and it == 'default')) AND dim1 > 1000");
    String expectedITPTrange = getWhereForDailyAndHourly2daysWithTimeDim(
      cubeName, "it", twodaysBack, now) + " OR (" +
      getWhereForDailyAndHourly2daysWithTimeDim(
        cubeName, "pt", twodaysBack, now) + ")";
    expected = getExpectedQuery(cubeName,
      "select testcube.dim1, avg(testcube.msr1), sum(testcube.msr2) FROM ",
      null, "AND testcube.it == 'default' and testcube.dim1 > 1000 group by testcube.dim1",
      expectedITPTrange, "C2_summary1");
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testFactsWithTimedDimensionWithProcessTimeCol() throws Exception {
    String twoDaysITRange = "time_range_in('it', '" +
      CubeTestSetup.getDateUptoHours(
      twodaysBack) + "','" + CubeTestSetup.getDateUptoHours(now) + "')";

    conf.set(CubeQueryConfUtil.PROCESS_TIME_PART_COL, "pt");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    String hqlQuery = rewrite(driver, "select dim1, AVG(msr1)," +
      " msr2 from testCube" +
      " where " + twoDaysITRange);
    System.out.println("Query With process time col:" + hqlQuery);
    //TODO compare queries
    //compareQueries(expected, hqlQuery);
    hqlQuery = rewrite(driver, "select dim1, dim2, COUNT(msr1)," +
      " SUM(msr2), msr3 from testCube" +
      " where " + twoDaysITRange);
    System.out.println("Query With process time col:" + hqlQuery);
    //TODO compare queries
    //compareQueries(expected, hqlQuery);
    hqlQuery = rewrite(driver, "select dim1, dim2, cityid, SUM(msr1)," +
      " SUM(msr2), msr3 from testCube" +
      " where " + twoDaysITRange);
    System.out.println("Query With process time col:" + hqlQuery);
    //TODO compare queries
    //compareQueries(expected, hqlQuery);
    conf.setInt(CubeQueryConfUtil.getLookAheadPTPartsKey(UpdatePeriod.DAILY), 3);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select dim1, AVG(msr1)," +
      " msr2 from testCube" +
      " where " + twoDaysITRange);
    System.out.println("Query With process time col:" + hqlQuery);
    //TODO compare queries
    //compareQueries(expected, hqlQuery);
    hqlQuery = rewrite(driver, "select dim1, dim2, COUNT(msr1)," +
      " SUM(msr2), msr3 from testCube" +
      " where " + twoDaysITRange);
    System.out.println("Query With process time col:" + hqlQuery);
    //TODO compare queries
    //compareQueries(expected, hqlQuery);
    hqlQuery = rewrite(driver, "select dim1, dim2, cityid, SUM(msr1)," +
      " SUM(msr2), msr3 from testCube" +
      " where " + twoDaysITRange);
    System.out.println("Query With process time col:" + hqlQuery);
    //TODO compare queries
    //compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeQueryWithMultipleRanges() throws Exception {
    String hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoDaysRange + " OR " + CubeTestSetup.twoDaysRangeBefore4days);

    String expectedRangeWhere = getWhereForDailyAndHourly2daysWithTimeDim(
      cubeName, "dt", twodaysBack, now) + " OR " +
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", CubeTestSetup.before4daysStart, CubeTestSetup.before4daysEnd);
    String expected = getExpectedQuery(cubeName,
      "select sum(testcube.msr2) FROM ", null, null,
      expectedRangeWhere, "c2_testfact");
    compareQueries(expected, hqlQuery);
    hqlQuery = rewrite(driver, "select dim1, AVG(msr1)," +
        " msr2 from testCube" +
        " where " + twoDaysRange + " OR " + CubeTestSetup.twoDaysRangeBefore4days);
    expected = getExpectedQuery(cubeName,
        "select testcube.dim1, avg(testcube.msr1), sum(testcube.msr2) FROM ",
        null, " group by testcube.dim1",
        expectedRangeWhere, "C1_summary1");
    compareQueries(expected, hqlQuery);
    hqlQuery = rewrite(driver, "select dim1, dim2, COUNT(msr1)," +
      " SUM(msr2), msr3 from testCube" +
      " where " + twoDaysRange + " OR " + CubeTestSetup.twoDaysRangeBefore4days);
    expected = getExpectedQuery(cubeName,
      "select testcube.dim1, testcube,dim2, count(testcube.msr1)," +
      " sum(testcube.msr2), max(testcube.msr3) FROM ", null,
      " group by testcube.dim1, testcube.dim2",
      expectedRangeWhere, "C1_summary2");
    compareQueries(expected, hqlQuery);
    hqlQuery = rewrite(driver, "select dim1, dim2, cityid, SUM(msr1)," +
        " SUM(msr2), msr3 from testCube" +
        " where " + twoDaysRange + " OR " + CubeTestSetup.twoDaysRangeBefore4days);
    expected = getExpectedQuery(cubeName,
        "select testcube.dim1, testcube,dim2, testcube.cityid," +
          " sum(testcube.msr1), sum(testcube.msr2), max(testcube.msr3) FROM ",
        null, " group by testcube.dim1, testcube.dim2, testcube.cityid",
        expectedRangeWhere, "C1_summary3");
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testDistinctColWithoutAlias() throws Exception {
    String hqlQuery = rewrite(driver, "select DISTINCT name, stateid" +
      " from citydim");
    String expected = getExpectedQuery("citydim", "select DISTINCT" +
      " citydim.name, citydim.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select id, sum(distinct id) from" +
      " citydim group by id");
    expected = getExpectedQuery("citydim", "select citydim.id," +
      " sum(DISTINCT citydim.id) from ", "group by citydim.id",
      "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select count(distinct id) from" +
      " citydim");
    expected = getExpectedQuery("citydim", "select count(DISTINCT" +
      " citydim.id) from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testJoinWithMultipleAliases() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, true);
    String cubeQl = "SELECT SUM(msr2) from testCube left outer join citydim c1 on testCube.cityid = c1.id" +
      " left outer join statedim s1 on c1.stateid = s1.id" +
      " left outer join citydim c2 on s1.countryid = c2.id where " + twoDaysRange;
    CubeQueryRewriter rewriter = new CubeQueryRewriter(conf);
    CubeQueryContext context  = rewriter.rewrite(cubeQl);
    String hql = context.toHQL();
    String db = getDbName();
    String expectedJoin = "FROM " + db + ".c3_testfact testcube " +
      "LEFT OUTER JOIN " + db + ".c1_citytable c1 ON (( testcube . cityid ) = ( c1 . id )) AND (c1.dt = 'latest') " +
      "LEFT OUTER JOIN " + db + ".c1_statetable s1 ON (( c1 . stateid ) = ( s1 . id )) AND (s1.dt = 'latest') " +
      "LEFT OUTER JOIN " + db + ".c1_citytable c2 ON (( s1 . countryid ) = ( c2 . id ))";

    Assert.assertTrue(hql.replaceAll("\\W+", "").contains(expectedJoin.replaceAll("\\W+", "")));
    System.out.println("%%% " + hql);
   }
}
