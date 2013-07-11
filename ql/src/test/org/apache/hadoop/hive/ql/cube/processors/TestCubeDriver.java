package org.apache.hadoop.hive.ql.cube.processors;

import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.now;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.twoMonthsBack;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.twodaysBack;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryConfUtil;
import org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup;
import org.apache.hadoop.hive.ql.cube.parse.DateUtil;
import org.apache.hadoop.hive.ql.cube.parse.StorageTableResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCubeDriver {

  private Configuration conf;
  private CubeDriver driver;
  private final String cubeName = "testcube";
  private final String twoDaysRange = "time_range_in('dt', '" + getDateUptoHours(
      twodaysBack) + "','" + getDateUptoHours(now) + "')";
  private final String twoMonthsRangeUptoHours = "time_range_in('dt', '" +
      getDateUptoHours(twoMonthsBack) + "','" + getDateUptoHours(now) + "')";
  private final String twoMonthsRangeUptoMonth = "time_range_in('dt', '" +
      getDateUptoMonth(twoMonthsBack) + "','" + getDateUptoMonth(now) + "')";

  static CubeTestSetup setup;
  static HiveConf hconf = new HiveConf(TestCubeDriver.class);
  @BeforeClass
  public static void setup() throws Exception {
    setup = new CubeTestSetup();
    setup.createSources(hconf, TestCubeDriver.class.getSimpleName());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    setup.dropSources(hconf);
  }

  @Before
  public void setupDriver() throws Exception {
    conf = new Configuration();
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
  }

  public static String HOUR_FMT = "yyyy-MM-dd HH";
  public static final SimpleDateFormat HOUR_PARSER = new SimpleDateFormat(
      HOUR_FMT);

  public static String MONTH_FMT = "yyyy-MM";
  public static final SimpleDateFormat MONTH_PARSER = new SimpleDateFormat(
      MONTH_FMT);

  public static String getDateUptoHours(Date dt) {
    return HOUR_PARSER.format(dt);
  }

  public static String getDateUptoMonth(Date dt) {
    return MONTH_PARSER.format(dt);
  }

  @Test
  public void testQueryWithNow() throws Exception {
    Throwable th = null;
    try {
      driver.compileCubeQuery("select SUM(msr2) from testCube where" +
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
      driver.compileCubeQuery("select dim12, SUM(msr2) from testCube" +
        " where " + twoDaysRange);
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    try {
      // this query should through exception because invalidMsr is invalid
      driver.compileCubeQuery("SELECT cityid, invalidMsr from testCube " +
        " where " + twoDaysRange);
      Assert.assertTrue("Should not reach here", false);
    } catch (SemanticException exc) {
      exc.printStackTrace();
      Assert.assertNotNull(exc);
    }

  }

  public String getExpectedQuery(String cubeName, String selExpr,
      String whereExpr, String postWhereExpr,
      Map<String, String> storageTableToWhereClause) {
    StringBuilder expected = new StringBuilder();
    int numTabs = storageTableToWhereClause.size();
    Assert.assertEquals(1, numTabs);
    for (Map.Entry<String, String> entry : storageTableToWhereClause.entrySet())
    {
      String storageTable = entry.getKey();
      expected.append(selExpr);
      expected.append(storageTable);
      expected.append(" ");
      expected.append(cubeName);
      expected.append(" WHERE ");
      expected.append("(");
      if (whereExpr != null) {
        expected.append(whereExpr);
        expected.append(" AND ");
      }
      expected.append(entry.getValue());
      expected.append(")");
      if (postWhereExpr != null) {
        expected.append(postWhereExpr);
      }
    }
    return expected.toString();
  }

  public String getExpectedQuery(String cubeName, String selExpr,
      String joinExpr, String whereExpr, String postWhereExpr,
      List<String> joinWhereConds,
      Map<String, String> storageTableToWhereClause) {
    StringBuilder expected = new StringBuilder();
    int numTabs = storageTableToWhereClause.size();
    Assert.assertEquals(1, numTabs);
    for (Map.Entry<String, String> entry : storageTableToWhereClause.entrySet())
    {
      String storageTable = entry.getKey();
      expected.append(selExpr);
      expected.append(storageTable);
      expected.append(" ");
      expected.append(cubeName);
      expected.append(joinExpr);
      expected.append(" WHERE ");
      expected.append("(");
      if (whereExpr != null) {
        expected.append(whereExpr);
        expected.append(" AND ");
      }
      expected.append(entry.getValue());
      if (joinWhereConds != null) {
        for (String joinEntry : joinWhereConds) {
          expected.append(" AND ");
          expected.append(joinEntry);
        }
      }
      expected.append(")");
      if (postWhereExpr != null) {
        expected.append(postWhereExpr);
      }
    }
    return expected.toString();
  }

  private Map<String, String> getWhereForDailyAndHourly2days(String cubeName,
      String storageTable) {
    Map<String, String> storageTableToWhereClause =
        new LinkedHashMap<String, String>();
    List<String> parts = new ArrayList<String>();
    Date dayStart;
    if (!CubeTestSetup.isZerothHour()) {
      addParts(parts, UpdatePeriod.HOURLY, twodaysBack,
          DateUtil.getCeilDate(twodaysBack, UpdatePeriod.DAILY));
      addParts(parts, UpdatePeriod.HOURLY,
          DateUtil.getFloorDate(now, UpdatePeriod.DAILY),
          DateUtil.getFloorDate(now, UpdatePeriod.HOURLY));
      dayStart = DateUtil.getCeilDate(
          twodaysBack, UpdatePeriod.DAILY);
    } else {
      dayStart = twodaysBack;
    }
    addParts(parts, UpdatePeriod.DAILY, dayStart,
        DateUtil.getFloorDate(now, UpdatePeriod.DAILY));
    storageTableToWhereClause.put(storageTable,
        StorageTableResolver.getWherePartClause("dt", cubeName, parts));
    return storageTableToWhereClause;
  }

  private Map<String, String> getWhereForMonthlyDailyAndHourly2months(
      String storageTable) {
    Map<String, String> storageTableToWhereClause =
        new LinkedHashMap<String, String>();
    List<String> parts = new ArrayList<String>();
    Date dayStart = twoMonthsBack;
    Date monthStart = twoMonthsBack;
    if (!CubeTestSetup.isZerothHour()) {
      addParts(parts, UpdatePeriod.HOURLY, twoMonthsBack,
          DateUtil.getCeilDate(twoMonthsBack, UpdatePeriod.DAILY));
      addParts(parts, UpdatePeriod.HOURLY,
          DateUtil.getFloorDate(now, UpdatePeriod.DAILY),
          DateUtil.getFloorDate(now, UpdatePeriod.HOURLY));
      dayStart = DateUtil.getCeilDate(
          twoMonthsBack, UpdatePeriod.DAILY);
    }
    Calendar cal = new GregorianCalendar();
    cal.setTime(dayStart);
    if (cal.get(Calendar.DAY_OF_MONTH) != 1) {
      addParts(parts, UpdatePeriod.DAILY, dayStart,
          DateUtil.getCeilDate(twoMonthsBack, UpdatePeriod.MONTHLY));
      monthStart = DateUtil.getCeilDate(twoMonthsBack, UpdatePeriod.MONTHLY);
    }
    addParts(parts, UpdatePeriod.DAILY,
          DateUtil.getFloorDate(now, UpdatePeriod.MONTHLY),
          DateUtil.getFloorDate(now, UpdatePeriod.DAILY));
    addParts(parts, UpdatePeriod.MONTHLY,
        monthStart,
        DateUtil.getFloorDate(now, UpdatePeriod.MONTHLY));
    storageTableToWhereClause.put(storageTable,
        StorageTableResolver.getWherePartClause("dt", cubeName, parts));
    return storageTableToWhereClause;
  }

  private Map<String, String> getWhereForMonthly2months(String monthlyTable) {
    Map<String, String> storageTableToWhereClause =
        new LinkedHashMap<String, String>();
    List<String> parts = new ArrayList<String>();
    addParts(parts, UpdatePeriod.MONTHLY,
        twoMonthsBack,
        DateUtil.getFloorDate(now, UpdatePeriod.MONTHLY));
    storageTableToWhereClause.put(monthlyTable,
        StorageTableResolver.getWherePartClause("dt", cubeName, parts));
    return storageTableToWhereClause;
  }

  private Map<String, String> getWhereForHourly2days(String hourlyTable) {
    Map<String, String> storageTableToWhereClause =
        new LinkedHashMap<String, String>();
    List<String> parts = new ArrayList<String>();
    addParts(parts, UpdatePeriod.HOURLY, twodaysBack,
        DateUtil.getFloorDate(now, UpdatePeriod.HOURLY));
    storageTableToWhereClause.put(hourlyTable,
        StorageTableResolver.getWherePartClause("dt", cubeName, parts));
    return storageTableToWhereClause;
  }

  private void addParts(List<String> partitions, UpdatePeriod updatePeriod,
      Date from, Date to) {
    String fmt = updatePeriod.format();
    Calendar cal = Calendar.getInstance();
    cal.setTime(from);
    Date dt = cal.getTime();
    while (dt.before(to)) {
      String part = new SimpleDateFormat(fmt).format(dt);
      cal.add(updatePeriod.calendarField(), 1);
      partitions.add(part);
      dt = cal.getTime();
    }
  }

  @Test
  public void testCubeExplain() throws Exception {
    String hqlQuery = driver.compileCubeQuery("explain select SUM(msr2) from " +
      "testCube where " + twoDaysRange);
    String expected = "EXPLAIN " + getExpectedQuery(cubeName,
      "select sum(testcube.msr2) FROM ", null, null,
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeInsert() throws Exception {
    String hqlQuery = driver.compileCubeQuery("insert overwrite directory" +
      " '/tmp/test' select SUM(msr2) from testCube where " + twoDaysRange);
    String expected = "insert overwrite directory '/tmp/test' " +
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("insert overwrite local directory" +
      " '/tmp/test' select SUM(msr2) from testCube where " + twoDaysRange);
    expected = "insert overwrite local directory '/tmp/test' " +
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("insert overwrite table temp" +
      " select SUM(msr2) from testCube where " + twoDaysRange);
    expected = "insert overwrite table temp " +
      getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ", null, null,
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
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
      System.err.println("\t__AGGR_EXPRS:" + driver.rewrittenQuery.getAggregateExprs());
    }
    Assert.assertTrue(expectedTrimmed.equalsIgnoreCase(actualTrimmed));
  }

  @Test
  public void testCubeWhereQuery() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    String expected = getExpectedQuery(cubeName,
      "select sum(testcube.msr2) FROM ", null, null,
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    // Test with partition existence
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c1_testfact2"));
    compareQueries(expected, hqlQuery);
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);

    // Tests for valid tables
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForDailyAndHourly2days(cubeName, "C2_testfact"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact2");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c1_testfact2"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact2");
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testFact2"),
        "C1_testFact2");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c1_testfact2"));
    compareQueries(expected, hqlQuery);


    conf.set(CubeQueryConfUtil.getValidFactTablesKey(cubeName), "testFact");
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"),
        "C1_testFact");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact",
        "C1"), "HOURLY");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c1_testfact"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    conf.set(CubeQueryConfUtil.getValidStorageTablesKey("testfact"),
        "C2_testFact");
    conf.set(CubeQueryConfUtil.getValidUpdatePeriodsKey("testfact", "C2"),
        "HOURLY");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c2_testfact"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeJoinQuery() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube"
      + " join citytable on testCube.cityid = citytable.id"
      + " where " + twoDaysRange);
    List<String> joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageTableResolver.getWherePartClause("dt",
        "citytable", Storage.getPartitionsForLatest()));
    String expected = getExpectedQuery(cubeName, "select sum(testcube.msr2)" +
      " FROM ", " INNER JOIN c1_citytable citytable ON" +
      " testCube.cityid = citytable.id", null, null, joinWhereConds,
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select statetable.name, SUM(msr2) from"
      + " testCube"
      + " join citytable on testCube.cityid = citytable.id"
      + " left outer join statetable on statetable.id = citytable.stateid"
      + " right outer join ziptable on citytable.zipcode = ziptable.code"
      + " where " + twoDaysRange);
    joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageTableResolver.getWherePartClause("dt",
        "citytable", Storage.getPartitionsForLatest()));
    joinWhereConds.add(StorageTableResolver.getWherePartClause("dt",
        "ziptable", Storage.getPartitionsForLatest()));
    expected = getExpectedQuery(cubeName, "select statetable.name," +
      " sum(testcube.msr2) FROM ", "INNER JOIN c1_citytable citytable ON" +
      " testCube.cityid = citytable.id LEFT OUTER JOIN c1_statetable statetable"
      + " ON statetable.id = citytable.stateid AND " +
      "(statetable.dt = 'latest') RIGHT OUTER JOIN c1_ziptable" +
      " ziptable ON citytable.zipcode = ziptable.code", null, " group by" +
      " statetable.name ", joinWhereConds,
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select st.name, SUM(msr2) from"
      + " testCube TC"
      + " join citytable CT on TC.cityid = CT.id"
      + " left outer join statetable ST on ST.id = CT.stateid"
      + " right outer join ziptable ZT on CT.zipcode = ZT.code"
      + " where " + twoDaysRange);
    joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageTableResolver.getWherePartClause("dt",
        "ct", Storage.getPartitionsForLatest()));
    joinWhereConds.add(StorageTableResolver.getWherePartClause("dt",
        "zt", Storage.getPartitionsForLatest()));
    expected = getExpectedQuery("tc", "select st.name," +
      " sum(tc.msr2) FROM ", " INNER JOIN c1_citytable ct ON" +
      " tc.cityid = ct.id LEFT OUTER JOIN c1_statetable st"
      + " ON st.id = ct.stateid and (st.dt = 'latest') " +
      "RIGHT OUTER JOIN c1_ziptable" +
      " zt ON ct.zipcode = zt.code", null, " group by" +
      " st.name ", joinWhereConds,
      getWhereForDailyAndHourly2days("tc", "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select statetable.name, SUM(msr2) from"
      + " testCube"
      + " left outer join citytable on testCube.cityid = citytable.id"
      + " left outer join ziptable on citytable.zipcode = ziptable.code"
      + " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select statetable.name," +
      " sum(testcube.msr2) FROM ", " LEFT OUTER JOIN c1_citytable citytable ON" +
      " testCube.cityid = citytable.id and (citytable.dt = 'latest') " +
      " LEFT OUTER JOIN c1_ziptable" +
      " ziptable ON citytable.zipcode = ziptable.code AND " +
      "(ziptable.dt = 'latest')", null, " group by" +
      " statetable.name ", null,
    getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube"
      + " join countrytable on testCube.countryid = countrytable.id"
      + " where " + twoMonthsRangeUptoMonth);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      " INNER JOIN c1_countrytable countrytable ON testCube.countryid = " +
      " countrytable.id", null, null, null,
        getWhereForMonthly2months("c2_testfactmonthly"));
    compareQueries(expected, hqlQuery);

    try {
      hqlQuery = driver.compileCubeQuery("select name, SUM(msr2) from testCube"
          + " join citytable" + " where " + twoDaysRange + " group by name");
      Assert.assertTrue(false);
    } catch (SemanticException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testCubeGroupbyQuery() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select name, SUM(msr2) from" +
      " testCube join citytable on testCube.cityid = citytable.id where " +
      twoDaysRange);
    List<String> joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageTableResolver.getWherePartClause("dt",
        "citytable", Storage.getPartitionsForLatest()));
    String expected = getExpectedQuery(cubeName, "select citytable.name," +
      " sum(testcube.msr2) FROM ", "INNER JOIN c1_citytable citytable ON" +
      " testCube.cityid = citytable.id", null, " group by citytable.name ",
      joinWhereConds, getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube"
      + " join citytable on testCube.cityid = citytable.id"
      + " where " + twoDaysRange + " group by name");
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select cityid, SUM(msr2) from testCube"
      + " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select testcube.cityid," +
      " sum(testcube.msr2) FROM ", null, " group by testcube.cityid ",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select round(cityid), SUM(msr2) from" +
      " testCube where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select round(testcube.cityid)," +
      " sum(testcube.msr2) FROM ", null, " group by round(testcube.cityid) ",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube"
      + "  where " + twoDaysRange + "group by round(zipcode)");
    expected = getExpectedQuery(cubeName, "select round(testcube.zipcode)," +
      " sum(testcube.msr2) FROM ", null, " group by round(testcube.zipcode) ",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select round(cityid), SUM(msr2) from" +
      " testCube where " + twoDaysRange + " group by zipcode");
    expected = getExpectedQuery(cubeName, "select testcube.zipcode," +
      " round(testcube.cityid), sum(testcube.msr2) FROM ", null,
      " group by testcube.zipcode, round(testcube.cityid)",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select cityid, SUM(msr2) from testCube"
      + " where " + twoDaysRange + " group by round(zipcode)");
    expected = getExpectedQuery(cubeName, "select round(testcube.zipcode)," +
      " testcube.cityid, sum(testcube.msr2) FROM ", null,
      " group by round(testcube.zipcode), testcube.cityid",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select cityid, msr2 from testCube"
      + " where " + twoDaysRange + " group by round(zipcode)");
    expected = getExpectedQuery(cubeName, "select round(testcube.zipcode)," +
      " testcube.cityid, sum(testcube.msr2) FROM ", null,
      " group by round(testcube.zipcode), testcube.cityid",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select round(zipcode) rzc, cityid," +
      " msr2 from testCube where " + twoDaysRange + " group by round(zipcode)" +
      " order by rzc");
    expected = getExpectedQuery(cubeName, "select round(testcube.zipcode) rzc,"
      + " testcube.cityid, sum(testcube.msr2) FROM ", null,
      " group by round(testcube.zipcode), testcube.cityid order by rzc",
    getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

  }

  @Test
  public void testCubeQueryWithAilas() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select SUM(msr2) m2 from" +
      " testCube where " + twoDaysRange);
    String expected = getExpectedQuery(cubeName, "select sum(testcube.msr2)" +
      " m2 FROM ", null, null, getWhereForDailyAndHourly2days(cubeName,
          "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube mycube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery("mycube", "select sum(mycube.msr2) FROM ", null,
      null, getWhereForDailyAndHourly2days("mycube", "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select SUM(testCube.msr2) from testCube"
      + " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select mycube.msr2 m2 from testCube" +
      " mycube where " + twoDaysRange);
    expected = getExpectedQuery("mycube", "select sum(mycube.msr2) m2 FROM ",
      null, null, getWhereForDailyAndHourly2days("mycube", "C1_testfact"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select testCube.msr2 m2 from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) m2 FROM ",
      null, null, getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeWhereQueryForMonth() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoMonthsRangeUptoHours);
    String expected = getExpectedQuery(cubeName,
      "select sum(testcube.msr2) FROM ", null, null,
      getWhereForMonthlyDailyAndHourly2months("C1_testfact"));
    compareQueries(expected, hqlQuery);

    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    try {
      hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
        " where " + twoMonthsRangeUptoHours);
      Assert.assertTrue(false);
    } catch (SemanticException e) {
      e.printStackTrace();
    }
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));

    // this should consider only two month partitions.
    hqlQuery = driver.compileCubeQuery("select cityid, SUM(msr2) from testCube"
      + " where " + twoMonthsRangeUptoMonth);
    expected = getExpectedQuery(cubeName, "select testcube.cityid," +
      " sum(testcube.msr2) FROM ", null, "group by testcube.cityid",
      getWhereForMonthly2months("c1_testfact"));
    compareQueries(expected, hqlQuery);
  }

  String getExpectedQuery(String dimName, String selExpr, String postWhereExpr,
      String storageTable, boolean hasPart) {
    StringBuilder expected = new StringBuilder();
    expected.append(selExpr);
    expected.append(storageTable);
    expected.append(" ");
    expected.append(dimName);
    if (hasPart) {
      expected.append(" WHERE ");
      expected.append(StorageTableResolver.getWherePartClause("dt",
          dimName, Storage.getPartitionsForLatest()));
    }
    if (postWhereExpr != null) {
      expected.append(postWhereExpr);
    }
    return expected.toString();
  }

  @Test
  public void testDimensionQueryWithMultipleStorages() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select name, stateid from" +
      " citytable");
    String expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select name, c.stateid from citytable" +
      " c");
    expected = getExpectedQuery("c", "select c.name, c.stateid from ", null,
      "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", null, "c2_citytable", false);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES, "C1_citytable");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES, "C2_citytable");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", null, "c2_citytable", false);
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select name n, count(1) from citytable"
      + " group by name order by n ");
    expected = getExpectedQuery("citytable", "select citytable.name n," +
      " count(1) from ", "groupby citytable.name order by n", "c2_citytable",
      false);
    compareQueries(expected, hqlQuery);

  }

  @Test
  public void testLimitQueryOnDimension() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select name, stateid from" +
      " citytable limit 100");
    String expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", " limit 100", "c1_citytable", true);
    compareQueries(expected, hqlQuery);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C2");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable " +
      "limit 100");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      "citytable.stateid from ", " limit 100", "c2_citytable", false);
    compareQueries(expected, hqlQuery);
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable" +
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
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    String expectedq2 = null;
    String expectedq3 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
     " sum(testCube.msr2) from ", null, "group by testcube.cityid",
     getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    String expectedq4 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) from ", null, "group by testcube.cityid having" +
      " sum(testCube.msr2) > 100",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    String expectedq5 = null;
    String expectedq6 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) from ", null, "group by testcube.cityid having" +
      " sum(testCube.msr2) > 100 and sum(testCube.msr2) < 1000",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    String expectedq7 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) from ", null, "group by testcube.cityid having" +
      " sum(testCube.msr2) > 100) OR (sum(testCube.msr2) < 100 AND" +
      " SUM(testcube.msr3) > 1000)",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    String expectedq8 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
        " sum(testCube.msr2) * sum(testCube.msr3) from ", null,
        "group by testcube.cityid",
        getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));
    String expectedq9 = getExpectedQuery(cubeName, "SELECT testcube.cityid c1,"
        + " sum(testCube.msr3) m3 from ", "c1 > 100", "group by testcube.cityid" +
        " having sum(testCube.msr2) < 100 AND (m3 > 1000)",
        getWhereForDailyAndHourly2days(cubeName, "C1_testfact"));

    String tests[] = {q1, q2, q3, q4, q5, q6, q7, q8, q9};
    String expected[] = {expectedq1, /*fail*/ expectedq2, expectedq3, expectedq4,
        /*fail*/ expectedq5, expectedq6, expectedq7, expectedq8, expectedq9};

    for (int i = 0; i < tests.length; i++) {
      String hql = null;
      Throwable th = null;
      try {
        hql = driver.compileCubeQuery(tests[i]);
      } catch (SemanticException e) {
        th = e;
        e.printStackTrace();
      }
      System.out.println("expected[" + i + "]:" + expected[i]);
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
      String hql = driver.compileCubeQuery(failq);
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
      String hql = driver.compileCubeQuery(query);
      Assert.assertTrue("Should not reach here:" + hql, false);
    } catch (SemanticException exc) {
      Assert.assertNotNull(exc);
      exc.printStackTrace();
    }

    String q2 = "SELECT ambigdim2 from citytable join" +
      " statetable on citytable.stateid = statetable.id join countrytable on" +
      " statetable.countryid = countrytable.id";
    try {
      String hql = driver.compileCubeQuery(q2);
      Assert.assertTrue("Should not reach here: " + hql, false);
    } catch (SemanticException exc) {
      Assert.assertNotNull(exc);
      exc.printStackTrace();
    }
  }

  @Test
  public void testTimeRangeValidation() throws Exception {
    System.out.println("##timerangein ");
    String timeRange2 = " time_range_in('dt', '" + getDateUptoHours(now)
        + "','" + getDateUptoHours(twodaysBack) + "')";
    try {
      // this should throw exception because from date is after to date
      driver.compileCubeQuery("SELECT cityid, testCube.msr2 from" +
          " testCube where " + timeRange2);
      Assert.assertTrue("Should not reach here", false);
    } catch (SemanticException exc) {
      exc.printStackTrace();
      Assert.assertNotNull(exc);
    }

    // check that time range can be any child of AND
    String timeRange = " time_range_in('dt', '" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')";
    String q1 = "SELECT cityid, testCube.msr2 from testCube where " + timeRange + " AND cityid=1";

    String hql = driver.compileCubeQuery(q1);
    // TODO compare queries

    String q2 = "SELECT cityid, testCube.msr3 from testCube where cityid=1 AND " + timeRange;

    hql = driver.compileCubeQuery(q2);
    //TODO compare queries

    // Check that column name in time range is extracted properly
    Assert.assertEquals("Time dimension should be " + Storage.getDatePartitionKey(),
        Storage.getDatePartitionKey(),
        driver.rewrittenQuery.getTimeDimension());
    
    // GRILL-38 NPE in extracting time range
    q1 = "SELECT cityid, testCube.msr2 from testCube where " + timeRange + " AND cityid IS NULL";
    driver.compileCubeQuery(q1);
    q1 = "SELECT cityid, testCube.msr2 from testCube where cityid IS NULL AND " + timeRange;
    driver.compileCubeQuery(q1);
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
          "C1_testfact")),
      getExpectedQuery(cubeName, "SELECT testCube.cityid, sum(testCube.msr2)" +
        " FROM ", " testcube.msr2 > 100 ", " group by testcube.cityid having" +
        " sum(testCube.msr2 < 1000)", getWhereForDailyAndHourly2days(
          cubeName, "C1_testfact")),
      getExpectedQuery(cubeName, "SELECT testCube.cityid, sum(testCube.msr2)" +
        " FROM ", " testcube.msr2 > 100 ", " group by testcube.cityid having" +
        " sum(testCube.msr2 < 1000) orderby testCube.cityid",
        getWhereForDailyAndHourly2days(cubeName, "C1_testfact")),
    };

    for (int i = 0; i < queries.length; i++) {
      String hql = driver.compileCubeQuery(queries[i]);
      compareQueries(expectedQueries[i], hql);
    }
  }

  @Test
  public void testFactsWithInvalidColumns() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select dim1, AVG(msr1)," +
      " msr2 from testCube" +
      " where " + twoDaysRange);
    String expected = getExpectedQuery(cubeName,
      "select testcube.dim1, avg(testcube.msr1), sum(testcube.msr2) FROM ",
      null, " group by testcube.dim1",
      getWhereForDailyAndHourly2days(cubeName, "C1_summary1"));
    compareQueries(expected, hqlQuery);
    hqlQuery = driver.compileCubeQuery("select dim1, dim2, COUNT(msr1)," +
      " SUM(msr2), msr3 from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName,
      "select testcube.dim1, testcube,dim2, count(testcube.msr1)," +
      " sum(testcube.msr2), max(testcube.msr3) FROM ", null,
      " group by testcube.dim1, testcube.dim2",
      getWhereForDailyAndHourly2days(cubeName, "C1_summary2"));
    compareQueries(expected, hqlQuery);
    hqlQuery = driver.compileCubeQuery("select dim1, dim2, cityid, SUM(msr1)," +
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
  public void testDistinctColWithoutAlias() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select DISTINCT name, stateid" +
      " from citytable");
    String expected = getExpectedQuery("citytable", "select DISTINCT" +
      " citytable.name, citytable.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select id, sum(distinct id) from" +
      " citytable group by id");
    expected = getExpectedQuery("citytable", "select citytable.id," +
      " sum(DISTINCT citytable.id) from ", "group by citytable.id",
      "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select count(distinct id) from" +
      " citytable");
    expected = getExpectedQuery("citytable", "select count(DISTINCT" +
      " citytable.id) from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);
  }
}
