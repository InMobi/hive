package org.apache.hadoop.hive.ql.cube.parse;

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
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
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
  private final String cubeName = CubeTestSetup.cubeName;

  static CubeTestSetup setup;
  static HiveConf hconf = new HiveConf(TestCubeRewriter.class);
  @BeforeClass
  public static void setup() throws Exception {
    SessionState.start(hconf);
    setup = new CubeTestSetup();
    setup.createSources(hconf, TestCubeRewriter.class.getSimpleName());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    setup.dropSources(hconf);
  }

  @Before
  public void setupDriver() throws Exception {
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2");
    conf.setBoolean(JoinResolver.DISABLE_AUTO_JOINS, true);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
  }

  @Test
  public void testQueryWithNow() throws Exception {
    Throwable th = null;
    try {
      rewrite(driver, "select SUM(msr2) from testCube where" +
        " time_range_in('dt', 'NOW - 2DAYS', 'NOW')");
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
  }

  @Test
  public void testCandidateTables() throws Exception {
    Throwable th = null;
    try {
      rewrite(driver, "select dim12, SUM(msr2) from testCube" +
        " where " + twoDaysRange);
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    try {
      // this query should through exception because invalidMsr is invalid
      rewrite(driver, "SELECT cityid, invalidMsr from testCube " +
        " where " + twoDaysRange);
      Assert.assertTrue("Should not reach here", false);
    } catch (SemanticException exc) {
      exc.printStackTrace();
      Assert.assertNotNull(exc);
    }

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

    // Query with column life not in the range
    Throwable th = null;
    try {
      hqlQuery = rewrite(driver, "cube select SUM(newmeasure) from testCube" +
        " where " + twoDaysRange);
    } catch (Exception e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertTrue(th instanceof SemanticException);
  }

  @Test
  public void testCubeInsert() throws Exception {
    String hqlQuery = rewrite(driver, "insert overwrite directory" +
      " '/tmp/test' select SUM(msr2) from testCube where " + twoDaysRange);
    String expected = "insert overwrite directory '/tmp/test' " +
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "insert overwrite local directory" +
      " '/tmp/test' select SUM(msr2) from testCube where " + twoDaysRange);
    expected = "insert overwrite local directory '/tmp/test' " +
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "insert overwrite table temp" +
      " select SUM(msr2) from testCube where " + twoDaysRange);
    expected = "insert overwrite table temp " +
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
        getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
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
        getWhereForDailyAndHourly2days(cubeName, "c2_testfact", "C1_testfact"));
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
        getWhereForMonthlyDailyAndHourly2months("c2_testfact","C1_testfact",
          "c3_testFact"));
    } else {
      expected = getExpectedQuery(cubeName,
        "select sum(testcube.msr2) FROM ", null, null,
        getWhereForMonthlyDailyAndHourly2months("c2_testfact", "c3_testFact"));
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
      + " join citytable on testCube.cityid = citytable.id"
      + " where " + twoDaysRange);
    List<String> joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt",
      "citytable", Storage.getPartitionsForLatest()));
    String expected = getExpectedQuery(cubeName, "select sum(testcube.msr2)" +
      " FROM ", " INNER JOIN c1_citytable citytable ON" +
      " testCube.cityid = citytable.id", null, null, joinWhereConds,
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    // q2
    hqlQuery = rewrite(driver, "select statetable.name, SUM(msr2) from"
      + " testCube"
      + " join citytable on testCube.cityid = citytable.id"
      + " left outer join statetable on statetable.id = citytable.stateid"
      + " right outer join ziptable on citytable.zipcode = ziptable.code"
      + " where " + twoDaysRange);
    joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt",
      "citytable", Storage.getPartitionsForLatest()));
    joinWhereConds.add(StorageUtil.getWherePartClause("dt",
      "ziptable", Storage.getPartitionsForLatest()));
    expected = getExpectedQuery(cubeName, "select statetable.name," +
      " sum(testcube.msr2) FROM ", "INNER JOIN c1_citytable citytable ON" +
      " testCube.cityid = citytable.id LEFT OUTER JOIN c1_statetable statetable"
      + " ON statetable.id = citytable.stateid AND " +
      "(statetable.dt = 'latest') RIGHT OUTER JOIN c1_ziptable" +
      " ziptable ON citytable.zipcode = ziptable.code", null, " group by" +
      " statetable.name ", joinWhereConds,
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    // q3
    hqlQuery = rewrite(driver, "select st.name, SUM(msr2) from"
      + " testCube TC"
      + " join citytable CT on TC.cityid = CT.id"
      + " left outer join statetable ST on ST.id = CT.stateid"
      + " right outer join ziptable ZT on CT.zipcode = ZT.code"
      + " where " + twoDaysRange);
    joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt",
      "ct", Storage.getPartitionsForLatest()));
    joinWhereConds.add(StorageUtil.getWherePartClause("dt",
      "zt", Storage.getPartitionsForLatest()));
    expected = getExpectedQuery("tc", "select st.name," +
      " sum(tc.msr2) FROM ", " INNER JOIN c1_citytable ct ON" +
      " tc.cityid = ct.id LEFT OUTER JOIN c1_statetable st"
      + " ON st.id = ct.stateid and (st.dt = 'latest') " +
      "RIGHT OUTER JOIN c1_ziptable" +
      " zt ON ct.zipcode = zt.code", null, " group by" +
      " st.name ", joinWhereConds,
      getWhereForDailyAndHourly2days("tc", "C2_testfact"));
    compareQueries(expected, hqlQuery);

    // q4
    hqlQuery = rewrite(driver, "select citytable.name, SUM(msr2) from"
      + " testCube"
      + " left outer join citytable on testCube.cityid = citytable.id"
      + " left outer join ziptable on citytable.zipcode = ziptable.code"
      + " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select citytable.name," +
      " sum(testcube.msr2) FROM ", " LEFT OUTER JOIN c1_citytable citytable ON" +
      " testCube.cityid = citytable.id and (citytable.dt = 'latest') " +
      " LEFT OUTER JOIN c1_ziptable" +
      " ziptable ON citytable.zipcode = ziptable.code AND " +
      "(ziptable.dt = 'latest')", null, " group by" +
      " citytable.name ", null,
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube"
      + " join countrytable on testCube.countryid = countrytable.id"
      + " where " + twoMonthsRangeUptoMonth);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      " INNER JOIN c1_countrytable countrytable ON testCube.countryid = " +
        " countrytable.id", null, null, null,
      getWhereForMonthly2months("c2_testfactmonthly"));
    compareQueries(expected, hqlQuery);

    try {
      hqlQuery = rewrite(driver, "select name, SUM(msr2) from testCube"
        + " join citytable" + " where " + twoDaysRange + " group by name");
      Assert.assertTrue(false);
    } catch (SemanticException e) {
      e.printStackTrace();
    }

  }

  @Test
  public void testCubeGroupbyQuery() throws Exception {
    String hqlQuery = rewrite(driver, "select name, SUM(msr2) from" +
      " testCube join citytable on testCube.cityid = citytable.id where " +
      twoDaysRange);
    List<String> joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageUtil.getWherePartClause("dt",
      "citytable", Storage.getPartitionsForLatest()));
    String expected = getExpectedQuery(cubeName, "select citytable.name," +
      " sum(testcube.msr2) FROM ", "INNER JOIN c1_citytable citytable ON" +
      " testCube.cityid = citytable.id", null, " group by citytable.name ",
      joinWhereConds, getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube"
      + " join citytable on testCube.cityid = citytable.id"
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
    expected = getExpectedQuery(cubeName, "select testcube.zipcode," +
      " round(testcube.cityid), sum(testcube.msr2) FROM ", null,
      " group by testcube.zipcode, round(testcube.cityid)",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select cityid, SUM(msr2) from testCube"
      + " where " + twoDaysRange + " group by round(zipcode)");
    expected = getExpectedQuery(cubeName, "select round(testcube.zipcode)," +
      " testcube.cityid, sum(testcube.msr2) FROM ", null,
      " group by round(testcube.zipcode), testcube.cityid",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select cityid, msr2 from testCube"
      + " where " + twoDaysRange + " group by round(zipcode)");
    expected = getExpectedQuery(cubeName, "select round(testcube.zipcode)," +
      " testcube.cityid, sum(testcube.msr2) FROM ", null,
      " group by round(testcube.zipcode), testcube.cityid",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select round(zipcode) rzc, cityid," +
      " msr2 from testCube where " + twoDaysRange + " group by round(zipcode)" +
      " order by rzc");
    expected = getExpectedQuery(cubeName, "select round(testcube.zipcode) rzc,"
      + " testcube.cityid, sum(testcube.msr2) FROM ", null,
      " group by round(testcube.zipcode), testcube.cityid order by rzc",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
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
    try {
      hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
        " where " + twoMonthsRangeUptoHours);
      Assert.assertTrue(false);
    } catch (SemanticException e) {
      e.printStackTrace();
    }
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
      " citytable");
    String expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select name, c.stateid from citytable" +
      " c");
    expected = getExpectedQuery("c", "select c.name, c.stateid from ", null,
      "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select name, stateid from citytable");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", null, "c2_citytable", false);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select name, stateid from citytable");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES, "C1_citytable");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select name, stateid from citytable");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES, "C2_citytable");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select name, stateid from citytable");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", null, "c2_citytable", false);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select name n, count(1) from citytable"
      + " group by name order by n ");
    expected = getExpectedQuery("citytable", "select citytable.name n," +
      " count(1) from ", "groupby citytable.name order by n", "c2_citytable",
      false);
    compareQueries(expected, hqlQuery);

  }

  @Test
  public void testLimitQueryOnDimension() throws Exception {
    String hqlQuery = rewrite(driver, "select name, stateid from" +
      " citytable limit 100");
    String expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", " limit 100", "c1_citytable", true);
    compareQueries(expected, hqlQuery);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select name, stateid from citytable " +
      "limit 100");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      "citytable.stateid from ", " limit 100", "c2_citytable", false);
    compareQueries(expected, hqlQuery);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select name, stateid from citytable" +
      " limit 100");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", " limit 100", "c1_citytable", true);
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testAggregateResolver() throws Exception {
    // pass
    String q1 = "SELECT cityid, testCube.msr2 from testCube where "
      + twoDaysRange;

    // fail
    String q2 = "SELECT cityid, testCube.msr2 * testCube.msr2 from testCube where "
      + twoDaysRange;

    // pass
    String q3 = "SELECT cityid, sum(testCube.msr2) from testCube where "
      + twoDaysRange;

    // pass
    String q4 = "SELECT cityid, sum(testCube.msr2) from testCube where "
      + twoDaysRange + " having testCube.msr2 > 100";

    // fail
    String q5 = "SELECT cityid, testCube.msr2 from testCube where "
      + twoDaysRange + " having testCube.msr2 + testCube.msr2 > 100";

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
    String expectedq2 = null;
    String expectedq3 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) from ", null, "group by testcube.cityid",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq4 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) from ", null, "group by testcube.cityid having" +
      " sum(testCube.msr2) > 100",
      getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    String expectedq5 = null;
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
    String expected[] = {expectedq1, /*fail*/ expectedq2, expectedq3, expectedq4,
        /*fail*/ expectedq5, expectedq6, expectedq7, expectedq8, expectedq9};

    for (int i = 0; i < tests.length; i++) {
      String hql = null;
      Throwable th = null;
      try {
        hql = rewrite(driver, tests[i]);
      } catch (SemanticException e) {
        th = e;
        e.printStackTrace();
      }
      if (expected[i] != null) {
        compareQueries(expected[i], hql);
      } else {
        Assert.assertNotNull(th);
      }
    }
    String failq = "SELECT cityid, testCube.noAggrMsr FROM testCube where "
      + twoDaysRange;
    try {
      // Should throw exception in aggregate resolver because noAggrMsr does
      //not have a default aggregate defined.
      String hql = rewrite(driver, failq);
      Assert.assertTrue("Should not reach here: " + hql, false);
    } catch (SemanticException exc) {
      Assert.assertNotNull(exc);
      exc.printStackTrace();
    }
  }

  @Test
  public void testColumnAmbiguity() throws Exception {
    String query = "SELECT ambigdim1, sum(testCube.msr1) FROM testCube join" +
      " citytable on testcube.cityid = citytable.id where " + twoDaysRange;

    try {
      String hql = rewrite(driver, query);
      Assert.assertTrue("Should not reach here:" + hql, false);
    } catch (SemanticException exc) {
      Assert.assertNotNull(exc);
      exc.printStackTrace();
    }

    String q2 = "SELECT ambigdim2 from citytable join" +
      " statetable on citytable.stateid = statetable.id join countrytable on" +
      " statetable.countryid = countrytable.id";
    try {
      String hql = rewrite(driver, q2);
      Assert.assertTrue("Should not reach here: " + hql, false);
    } catch (SemanticException exc) {
      Assert.assertNotNull(exc);
      exc.printStackTrace();
    }
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
        " sum(testCube.msr2 < 1000) orderby testCube.cityid",
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

    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, -4);
    Date end = cal.getTime();
    cal.add(Calendar.DAY_OF_MONTH, -2);
    Date start = cal.getTime();
    String twoDaysRangeBefore4days = "time_range_in('dt', '" +
        CubeTestSetup.getDateUptoHours(
        start) + "','" + CubeTestSetup.getDateUptoHours(end) + "')";
    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoDaysRange + " OR (" + twoDaysRangeBefore4days + " AND dt='default')");

    String expecteddtRangeWhere1 = getWhereForDailyAndHourly2daysWithTimeDim(
      cubeName, "dt", twodaysBack, now) + " OR (" +
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", start, end) + ")";
    expected = getExpectedQuery(cubeName,
      "select sum(testcube.msr2) FROM ", null, " AND testcube.dt='default'",
      expecteddtRangeWhere1, "c2_testfact");
    compareQueries(expected, hqlQuery);

    String expecteddtRangeWhere2 = "(" + getWhereForDailyAndHourly2daysWithTimeDim(
      cubeName, "dt", twodaysBack, now) + " AND testcube.dt='dt1') OR " +
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", start, end);
    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where (" + twoDaysRange + " AND dt='dt1') OR (" +
      twoDaysRangeBefore4days + " AND dt='default')");
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
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, -4);
    Date end = cal.getTime();
    cal.add(Calendar.DAY_OF_MONTH, -2);
    Date start = cal.getTime();
    String twoDaysRangeBefore4days = "time_range_in('dt', '" +
        CubeTestSetup.getDateUptoHours(
        start) + "','" + CubeTestSetup.getDateUptoHours(end) + "')";
    String hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
      " where " + twoDaysRange + " OR " + twoDaysRangeBefore4days);

    String expectedRangeWhere = getWhereForDailyAndHourly2daysWithTimeDim(
      cubeName, "dt", twodaysBack, now) + " OR " +
      getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", start, end);
    String expected = getExpectedQuery(cubeName,
      "select sum(testcube.msr2) FROM ", null, null,
      expectedRangeWhere, "c2_testfact");
    compareQueries(expected, hqlQuery);
    hqlQuery = rewrite(driver, "select dim1, AVG(msr1)," +
        " msr2 from testCube" +
        " where " + twoDaysRange + " OR " + twoDaysRangeBefore4days);
    expected = getExpectedQuery(cubeName,
        "select testcube.dim1, avg(testcube.msr1), sum(testcube.msr2) FROM ",
        null, " group by testcube.dim1",
        expectedRangeWhere, "C1_summary1");
    compareQueries(expected, hqlQuery);
    hqlQuery = rewrite(driver, "select dim1, dim2, COUNT(msr1)," +
      " SUM(msr2), msr3 from testCube" +
      " where " + twoDaysRange + " OR " + twoDaysRangeBefore4days);
    expected = getExpectedQuery(cubeName,
      "select testcube.dim1, testcube,dim2, count(testcube.msr1)," +
      " sum(testcube.msr2), max(testcube.msr3) FROM ", null,
      " group by testcube.dim1, testcube.dim2",
      expectedRangeWhere, "C1_summary2");
    compareQueries(expected, hqlQuery);
    hqlQuery = rewrite(driver, "select dim1, dim2, cityid, SUM(msr1)," +
        " SUM(msr2), msr3 from testCube" +
        " where " + twoDaysRange + " OR " + twoDaysRangeBefore4days);
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
      " from citytable");
    String expected = getExpectedQuery("citytable", "select DISTINCT" +
      " citytable.name, citytable.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select id, sum(distinct id) from" +
      " citytable group by id");
    expected = getExpectedQuery("citytable", "select citytable.id," +
      " sum(DISTINCT citytable.id) from ", "group by citytable.id",
      "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select count(distinct id) from" +
      " citytable");
    expected = getExpectedQuery("citytable", "select count(DISTINCT" +
      " citytable.id) from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);
  }
}