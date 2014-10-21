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
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.getWhereForHourly2days;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.twoDaysRange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBaseCubeQueries {

  private Configuration conf;
  private CubeQueryRewriter driver;
  private final String cubeName = CubeTestSetup.BASE_CUBE_NAME;

  static CubeTestSetup setup;
  static HiveConf hconf = new HiveConf(TestBaseCubeQueries.class);
  static String dbName;
  @BeforeClass
  public static void setup() throws Exception {
    SessionState.start(hconf);
    setup = new CubeTestSetup();
    String dbName = TestBaseCubeQueries.class.getSimpleName();
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
    driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
  }

  @Test
  public void testColumnErrors() throws Exception {
    SemanticException th = null;
    try {
      rewrite(driver, "select dim2, SUM(msr1) from basecube" +
          " where " + twoDaysRange);
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.FIELDS_NOT_QUERYABLE.getErrorCode());
    Assert.assertTrue(
        th.getMessage().contains("dim2") && th.getMessage().contains("msr1"));

    th = null;
    try {
      rewrite(driver, "select dim2, cityid, SUM(msr2) from basecube" +
          " where " + twoDaysRange);
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.FIELDS_NOT_QUERYABLE.getErrorCode());
    Assert.assertTrue(th.getMessage().contains("dim2") && th.getMessage().contains("cityid"));

    th = null;
    try {
      rewrite(driver, "select newmeasure from basecube" +
          " where " + twoDaysRange);
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    Assert.assertEquals(th.getCanonicalErrorMsg().getErrorCode(),
        ErrorMsg.FIELDS_NOT_QUERYABLE.getErrorCode());
    Assert.assertTrue(th.getMessage().contains("newmeasure"));
  }

  @Test
  public void testCommonDimensions() throws Exception {
    String hqlQuery = rewrite(driver, "select dim1, SUM(msr1) from basecube" +
        " where " + twoDaysRange);
    System.out.println("HQL:" + hqlQuery);
    String expected = getExpectedQuery(cubeName,
        "select basecube.dim1, SUM(basecube.msr1) FROM ", null, " group by basecube.dim1",
        getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    TestCubeRewriter.compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select dim1, SUM(msr1), msr2 from basecube" +
        " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName,
        "select basecube.dim1, SUM(basecube.msr1), basecube.msr2 FROM ", null,
        " group by basecube.dim1",
        getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select dim1, roundedmsr2 from basecube" +
        " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName,
        "select basecube.dim1, round(sum(basecube.msr2)/1000) FROM ", null,
        " group by basecube.dim1",
        getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(expected, hqlQuery);

    hqlQuery = rewrite(driver, "select booleancut, msr2 from basecube" +
        " where " + twoDaysRange + " and substrexpr != 'XYZ'");
    expected = getExpectedQuery(cubeName,
        "select basecube.dim1 != 'x' AND basecube.dim2 != 10 ," +
            " sum(basecube.msr2) FROM ", null,
            " and substr(basecube.dim1, 3) != 'XYZ' " +
                "group by basecube.dim1 != 'x' AND basecube.dim2 != 10",
                getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    System.out.println("HQL:" + hqlQuery);
    TestCubeRewriter.compareQueries(expected, hqlQuery);
  }

  private CubeQueryContext rewrittenQuery;
  private String rewrite(CubeQueryRewriter driver, String query)
      throws SemanticException, ParseException {
    rewrittenQuery = driver.rewrite(query);
    return rewrittenQuery.toHQL();
  }
}
