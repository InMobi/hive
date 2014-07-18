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


import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.getExpectedQuery;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.twoDaysRange;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTimeRangeWriterWithQuery {

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
    String dbName = TestTimeRangeWriterWithQuery.class.getSimpleName();
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
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
    conf.setClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS,
        BetweenTimeRangeWriter.class.asSubclass(TimeRangeWriter.class),
        TimeRangeWriter.class);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
  }

  private CubeQueryContext rewrittenQuery;
  private String rewrite(CubeQueryRewriter driver, String query)
      throws SemanticException, ParseException {
    rewrittenQuery = driver.rewrite(query);
    return rewrittenQuery.toHQL();
  }

  private Date getOneLess(Date in, int calField) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(in);
    cal.add(calField, -1);
    return cal.getTime();
  }

  private Date getUptoHour(Date in) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(in);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime(); 
  }

  @Test
  public void testCubeQuery() throws Exception {
    SemanticException th = null;
    try {
      rewrite(driver, "cube select" +
          " SUM(msr2) from testCube where " + twoDaysRange);
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.CANNOT_USE_TIMERANGE_WRITER.getErrorCode());

    // hourly partitions for two days
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    String hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
        " where " + twoDaysRange);
    Map<String, String> whereClauses = new HashMap<String, String>();
    whereClauses.put(CubeTestSetup.getDbName() + "c1_testfact2",
        TestBetweenTimeRangeWriter.getBetweenClause(cubeName, "dt",
        CubeTestSetup.twodaysBack, getOneLess(CubeTestSetup.now,
            UpdatePeriod.HOURLY.calendarField()), UpdatePeriod.HOURLY.format()));
    String expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
        null, null, whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(expected, hqlQuery);

    // multiple range query
    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
        " where " + twoDaysRange + " OR " + CubeTestSetup.twoDaysRangeBefore4days);

    whereClauses = new HashMap<String, String>();
    whereClauses.put(CubeTestSetup.getDbName() + "c1_testfact2",
        TestBetweenTimeRangeWriter.getBetweenClause(cubeName, "dt",
        CubeTestSetup.twodaysBack, getOneLess(CubeTestSetup.now,
            UpdatePeriod.HOURLY.calendarField()), UpdatePeriod.HOURLY.format()) +
        " OR" + TestBetweenTimeRangeWriter.getBetweenClause(cubeName, "dt",
            CubeTestSetup.before4daysStart, getOneLess(CubeTestSetup.before4daysEnd,
                UpdatePeriod.HOURLY.calendarField()),
            UpdatePeriod.HOURLY.format())
        );
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
        null, null, whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(expected, hqlQuery);

    // format option in the query
    conf.set(CubeQueryConfUtil.PART_WHERE_CLAUSE_DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    hqlQuery = rewrite(driver, "select SUM(msr2) from testCube" +
        " where " + twoDaysRange);
    whereClauses = new HashMap<String, String>();
    whereClauses.put(CubeTestSetup.getDbName() + "c1_testfact2",
        TestBetweenTimeRangeWriter.getBetweenClause(cubeName, "dt",
            getUptoHour(CubeTestSetup.twodaysBack), getUptoHour(getOneLess(CubeTestSetup.now,
            UpdatePeriod.HOURLY.calendarField())), TestTimeRangeWriter.dbFormat));
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
        null, null, whereClauses);
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(expected, hqlQuery);
  }
}
