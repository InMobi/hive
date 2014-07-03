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


import static org.junit.Assert.assertEquals;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.BaseDimension;
import org.apache.hadoop.hive.ql.cube.metadata.ColumnMeasure;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimension;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMeasure;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.ExprMeasure;
import org.apache.hadoop.hive.ql.cube.metadata.HDFSStorage;
import org.apache.hadoop.hive.ql.cube.metadata.HierarchicalDimension;
import org.apache.hadoop.hive.ql.cube.metadata.InlineDimension;
import org.apache.hadoop.hive.ql.cube.metadata.MetastoreConstants;
import org.apache.hadoop.hive.ql.cube.metadata.MetastoreUtil;
import org.apache.hadoop.hive.ql.cube.metadata.ReferencedDimension;
import org.apache.hadoop.hive.ql.cube.metadata.StorageConstants;
import org.apache.hadoop.hive.ql.cube.metadata.StoragePartitionDesc;
import org.apache.hadoop.hive.ql.cube.metadata.StorageTableDesc;
import org.apache.hadoop.hive.ql.cube.metadata.TableReference;
import org.apache.hadoop.hive.ql.cube.metadata.TestCubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.UberDimension;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.TextInputFormat;

/*
 * Here is the cube test setup
 *
 * Cube : testCube
 *
 * Fact : testFact
 *
 * Fact storage and Updates:
 * testFact : {C1, C2, C3} -> {Minutely, hourly, daily, monthly, quarterly, yearly}
 * testFact2 : {C1} -> {Hourly}
 * testFactMonthly : {C2} -> {Monthly}
 * summary1,summary2,summary3 - {C1, C2} -> {daily, hourly, minutely}
 *   C2 has multiple dated partitions
 *
 * CityTable : C1 - SNAPSHOT and C2 - NO snapshot
 */

@SuppressWarnings("deprecation")
public class CubeTestSetup {

  public static String HOUR_FMT = "yyyy-MM-dd-HH";
  public static final SimpleDateFormat HOUR_PARSER = new SimpleDateFormat(
      HOUR_FMT);
  public static String MONTH_FMT = "yyyy-MM";
  public static final SimpleDateFormat MONTH_PARSER = new SimpleDateFormat(
      MONTH_FMT);
  private Set<CubeMeasure> cubeMeasures;
  private Set<CubeDimension> cubeDimensions;
  public static final String TEST_CUBE_NAME= "testCube";
  public static final String DERIVED_CUBE_NAME= "derivedCube";
  public static Date now;
  public static Date twodaysBack;
  public static String twoDaysRange;
  public static Date twoMonthsBack;
  public static String twoMonthsRangeUptoMonth;
  public static String twoMonthsRangeUptoHours;
  public static String twoDaysRangeBefore4days;
  public static Date before4daysStart;
  public static Date before4daysEnd;
  private static boolean zerothHour;
  public static Map<String, String> dimProps = new HashMap<String, String>();
  private static String c1 = "C1";
  private static String c2 = "C2";
  private static String c3 = "C3";

  public static void init () {
    Calendar cal = Calendar.getInstance();
    now = cal.getTime();
    zerothHour = (cal.get(Calendar.HOUR_OF_DAY) == 0);
    System.out.println("Test now:" + now);
    cal.add(Calendar.DAY_OF_MONTH, -2);
    twodaysBack = cal.getTime();
    System.out.println("Test twodaysBack:" + twodaysBack);
   
    // two months back
    cal.setTime(now);
    cal.add(Calendar.MONTH, -2);
    twoMonthsBack = cal.getTime();
    System.out.println("Test twoMonthsBack:" + twoMonthsBack);

    // Before 4days
    cal.setTime(now);
    cal.add(Calendar.DAY_OF_MONTH, -4);
    before4daysEnd = cal.getTime();
    cal.add(Calendar.DAY_OF_MONTH, -2);
    before4daysStart = cal.getTime();
    twoDaysRangeBefore4days = "time_range_in('dt', '" +
        CubeTestSetup.getDateUptoHours(before4daysStart) + "','" +
        CubeTestSetup.getDateUptoHours(before4daysEnd) + "')";

    twoDaysRange = "time_range_in('dt', '" + getDateUptoHours(
        twodaysBack) + "','" + getDateUptoHours(now) + "')";
    twoMonthsRangeUptoMonth = "time_range_in('dt', '" +
        getDateUptoMonth(twoMonthsBack) + "','" + getDateUptoMonth(now) + "')";
    twoMonthsRangeUptoHours = "time_range_in('dt', '" +
        getDateUptoHours(twoMonthsBack) + "','" + getDateUptoHours(now) + "')";

    dimProps.put(MetastoreConstants.TIMED_DIMENSION,
        TestCubeMetastoreClient.getDatePartitionKey());
  }

  private static boolean inited;

  public CubeTestSetup() {
    if (!inited) {
      init();
      inited = true;
    }
  }

  public static boolean isZerothHour() {
    return zerothHour;
  }

  public static String getDateUptoHours(Date dt) {
    return HOUR_PARSER.format(dt);
  }

  public static String getDateUptoMonth(Date dt) {
    return MONTH_PARSER.format(dt);
  }

  public static String getExpectedQuery(String cubeName, String selExpr,
      String whereExpr, String postWhereExpr,
      Map<String, String> storageTableToWhereClause) {
    StringBuilder expected = new StringBuilder();
    int numTabs = storageTableToWhereClause.size();
    assertEquals(1, numTabs);
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

  public static String getExpectedQuery(String cubeName, String selExpr,
      String whereExpr, String postWhereExpr,
      String rangeWhere, String storageTable) {
    StringBuilder expected = new StringBuilder();
    expected.append(selExpr);
    expected.append(getDbName() + storageTable);
    expected.append(" ");
    expected.append(cubeName);
    expected.append(" WHERE ");
    expected.append("(");
    if (whereExpr != null) {
      expected.append(whereExpr);
      expected.append(" AND ");
    }
    expected.append(rangeWhere);
    expected.append(")");
    if (postWhereExpr != null) {
      expected.append(postWhereExpr);
    }
    return expected.toString();
  }

  public static String getExpectedQuery(String cubeName, String selExpr,
      String joinExpr, String whereExpr, String postWhereExpr,
      List<String> joinWhereConds,
      Map<String, String> storageTableToWhereClause) {
    StringBuilder expected = new StringBuilder();
    int numTabs = storageTableToWhereClause.size();
    assertEquals(1, numTabs);
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

  public static Map<String, String> getWhereForDailyAndHourly2days(String cubeName,
      String... storageTables) {
    return getWhereForDailyAndHourly2daysWithTimeDim(cubeName, "dt", storageTables);
  }

  public static String getDbName() {
    String database = SessionState.get().getCurrentDatabase();
    if (!"default".equalsIgnoreCase(database) && StringUtils.isNotBlank(database)) {
      return database + ".";
    }
    return "";
  }

  public static Map<String, String> getWhereForDailyAndHourly2daysWithTimeDim(
      String cubeName, String timedDimension, String... storageTables) {
    return getWhereForDailyAndHourly2daysWithTimeDim(cubeName, timedDimension,
        twodaysBack, now, storageTables);
  }

  public static Map<String, String> getWhereForDailyAndHourly2daysWithTimeDim(
      String cubeName, String timedDimension, Date from, Date to, String... storageTables) {
    Map<String, String> storageTableToWhereClause =
        new LinkedHashMap<String, String>();
    String whereClause = getWhereForDailyAndHourly2daysWithTimeDim(cubeName,
        timedDimension, from,
        to);
    storageTableToWhereClause.put(getStorageTableString(storageTables),
        whereClause);
    return storageTableToWhereClause;
  }

  private static String getStorageTableString(String... storageTables) {
    String dbName = getDbName();
    if (!StringUtils.isBlank(dbName)) {
      List<String> tbls = new ArrayList<String>();
      for (String tbl : storageTables) {
        tbls.add(dbName + tbl);
      }
      return  StringUtils.join(tbls, ",");
    }
    return StringUtils.join(storageTables, ",");
  }
  public static String getWhereForDailyAndHourly2daysWithTimeDim(
      String cubeName, String timedDimension, Date from, Date to) {
    List<String> hourlyparts = new ArrayList<String>();
    List<String> dailyparts = new ArrayList<String>();
    Date dayStart;
    if (!CubeTestSetup.isZerothHour()) {
      addParts(hourlyparts, UpdatePeriod.HOURLY, from,
          DateUtil.getCeilDate(from, UpdatePeriod.DAILY));
      addParts(hourlyparts, UpdatePeriod.HOURLY,
          DateUtil.getFloorDate(to, UpdatePeriod.DAILY),
          DateUtil.getFloorDate(to, UpdatePeriod.HOURLY));
      dayStart = DateUtil.getCeilDate(
          from, UpdatePeriod.DAILY);
    } else {
      dayStart = from;
    }
    addParts(dailyparts, UpdatePeriod.DAILY, dayStart,
        DateUtil.getFloorDate(to, UpdatePeriod.DAILY));
    List<String> parts = new ArrayList<String>();
    parts.addAll(hourlyparts);
    parts.addAll(dailyparts);
    Collections.sort(parts);
    return  StorageUtil.getWherePartClause(timedDimension, cubeName, parts);
  }

  public static Map<String, String> getWhereForMonthlyDailyAndHourly2months(
      String... storageTables) {
    Map<String, String> storageTableToWhereClause =
        new LinkedHashMap<String, String>();
    List<String> hourlyparts = new ArrayList<String>();
    List<String> dailyparts = new ArrayList<String>();
    List<String> monthlyparts = new ArrayList<String>();
    Date dayStart = twoMonthsBack;
    Date monthStart = twoMonthsBack;
    if (!CubeTestSetup.isZerothHour()) {
      addParts(hourlyparts, UpdatePeriod.HOURLY, twoMonthsBack,
          DateUtil.getCeilDate(twoMonthsBack, UpdatePeriod.DAILY));
      addParts(hourlyparts, UpdatePeriod.HOURLY,
          DateUtil.getFloorDate(now, UpdatePeriod.DAILY),
          DateUtil.getFloorDate(now, UpdatePeriod.HOURLY));
      dayStart = DateUtil.getCeilDate(
          twoMonthsBack, UpdatePeriod.DAILY);
      monthStart = DateUtil.getCeilDate(twoMonthsBack, UpdatePeriod.MONTHLY);
    }
    Calendar cal = new GregorianCalendar();
    cal.setTime(dayStart);
    if (cal.get(Calendar.DAY_OF_MONTH) != 1) {
      addParts(dailyparts, UpdatePeriod.DAILY, dayStart,
          DateUtil.getCeilDate(twoMonthsBack, UpdatePeriod.MONTHLY));
      monthStart = DateUtil.getCeilDate(twoMonthsBack, UpdatePeriod.MONTHLY);
    }
    addParts(dailyparts, UpdatePeriod.DAILY,
        DateUtil.getFloorDate(now, UpdatePeriod.MONTHLY),
        DateUtil.getFloorDate(now, UpdatePeriod.DAILY));
    addParts(monthlyparts, UpdatePeriod.MONTHLY,
        monthStart,
        DateUtil.getFloorDate(now, UpdatePeriod.MONTHLY));
    List<String> parts = new ArrayList<String>();
    parts.addAll(dailyparts);
    parts.addAll(hourlyparts);
    parts.addAll(monthlyparts);
    StringBuilder tables = new StringBuilder();
    if (storageTables.length > 1) {
      if (!hourlyparts.isEmpty()) {
        tables.append(getDbName());
        tables.append(storageTables[0]);
        tables.append(",");
      }
      if (!dailyparts.isEmpty()) {
        tables.append(getDbName());
        tables.append(storageTables[1]);
        tables.append(",");
      }
      tables.append(getDbName());
      tables.append(storageTables[2]);
    } else {
      tables.append(getDbName());
      tables.append(storageTables[0]);
    }
    Collections.sort(parts);
    storageTableToWhereClause.put(tables.toString(),
        StorageUtil.getWherePartClause("dt", TEST_CUBE_NAME, parts));
    return storageTableToWhereClause;
  }

  public static Map<String, String> getWhereForMonthly2months(String monthlyTable) {
    Map<String, String> storageTableToWhereClause =
        new LinkedHashMap<String, String>();
    List<String> parts = new ArrayList<String>();
    addParts(parts, UpdatePeriod.MONTHLY,
        twoMonthsBack,
        DateUtil.getFloorDate(now, UpdatePeriod.MONTHLY));
    storageTableToWhereClause.put(getDbName() + monthlyTable,
        StorageUtil.getWherePartClause("dt", TEST_CUBE_NAME, parts));
    return storageTableToWhereClause;
  }

  public static Map<String, String> getWhereForHourly2days(String hourlyTable) {
    Map<String, String> storageTableToWhereClause =
        new LinkedHashMap<String, String>();
    List<String> parts = new ArrayList<String>();
    addParts(parts, UpdatePeriod.HOURLY, twodaysBack,
        DateUtil.getFloorDate(now, UpdatePeriod.HOURLY));
    storageTableToWhereClause.put(getDbName() + hourlyTable,
        StorageUtil.getWherePartClause("dt", TEST_CUBE_NAME, parts));
    return storageTableToWhereClause;
  }

  public static void addParts(List<String> partitions, UpdatePeriod updatePeriod,
      Date from, Date to) {
    DateFormat fmt = updatePeriod.format();
    Calendar cal = Calendar.getInstance();
    cal.setTime(from);
    Date dt = cal.getTime();
    while (dt.before(to)) {
      String part = fmt.format(dt);
      cal.add(updatePeriod.calendarField(), 1);
      partitions.add(part);
      dt = cal.getTime();
    }
  }

  public static String rewrite(CubeQueryRewriter driver, String query)
      throws SemanticException, ParseException {
    CubeQueryContext rewrittenQuery = driver.rewrite(query);
    return rewrittenQuery.toHQL();
  }

  public static String getExpectedQuery(String dimName, String selExpr, String postWhereExpr,
      String storageTable, boolean hasPart) {
    return getExpectedQuery(dimName, selExpr, null, postWhereExpr, storageTable, hasPart);
  }

  public static String getExpectedQuery(String dimName, String selExpr, String whereExpr,
      String postWhereExpr, String storageTable, boolean hasPart) {
    StringBuilder expected = new StringBuilder();
    String partWhere = null;
    if (hasPart) {
      partWhere = StorageUtil.getWherePartClause("dt",
          dimName, StorageConstants.getPartitionsForLatest());
    }
    expected.append(selExpr);
    expected.append(getDbName() + storageTable);
    expected.append(" ");
    expected.append(dimName);
    if (whereExpr != null || hasPart) {
      expected.append(" WHERE ");
      expected.append("(");
      if (whereExpr != null) {
        expected.append(whereExpr);
        if (partWhere != null) {
          expected.append(" AND ");
        }
      }
      if (partWhere != null) {
        expected.append(partWhere);
      }
      expected.append(")");
    }
    if (postWhereExpr != null) {
      expected.append(postWhereExpr);
    }
    return expected.toString();
  }

  private void createCube(CubeMetastoreClient client) throws HiveException {
    cubeMeasures = new HashSet<CubeMeasure>();
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr1", "int",
        "first measure")));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr2", "float",
        "second measure"),
        null, "SUM", "RS"));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr3", "double",
        "third measure"),
        null, "MAX", null));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr4", "bigint",
        "fourth measure"),
        null, "COUNT", null));
    cubeMeasures.add(new ExprMeasure(new FieldSchema("msr5", "double",
        "fifth measure"),
        "avg(msr1 + msr2)"));
    cubeMeasures.add(new ExprMeasure(new FieldSchema("msr6", "bigint",
        "sixth measure"),
        "(msr1 + msr2)/ msr4", "", "SUM", "RS"));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("noAggrMsr", "bigint",
        "measure without a default aggregate"),
        null, null, null
        ));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("newmeasure", "bigint",
        "measure available  from now"), null, null, null, now, null, 100.0));

    cubeDimensions = new HashSet<CubeDimension>();
    List<CubeDimension> locationHierarchy = new ArrayList<CubeDimension>();
    locationHierarchy.add(new ReferencedDimension(new FieldSchema("zipcode",
        "int", "zip"), new TableReference("zipdim", "code")));
    locationHierarchy.add(new ReferencedDimension(new FieldSchema("cityid",
        "int", "city"), new TableReference("citydim", "id")));
    locationHierarchy.add(new ReferencedDimension(new FieldSchema("stateid",
        "int", "state"), new TableReference("statedim", "id")));
    locationHierarchy.add(new ReferencedDimension(new FieldSchema("countryid",
        "int", "country"), new TableReference("countrydim", "id")));
    List<String> regions = Arrays.asList("APAC", "EMEA", "USA");
    locationHierarchy.add(new InlineDimension(new FieldSchema("regionname",
        "string", "region"), regions));

    cubeDimensions.add(new HierarchicalDimension("location", locationHierarchy));
    cubeDimensions.add(new BaseDimension(new FieldSchema("dim1", "string",
        "basedim")));
    // Added for ambiguity test
    cubeDimensions.add(new BaseDimension(new FieldSchema("ambigdim1", "string",
        "used in testColumnAmbiguity")));
    cubeDimensions.add(new ReferencedDimension(
        new FieldSchema("dim2", "string", "ref dim"),
        new TableReference("testdim2", "id")));
    Map<String, String> cubeProperties =
        new HashMap<String, String>();
    cubeProperties.put(MetastoreUtil.getCubeTimedDimensionListKey(
        TEST_CUBE_NAME), "dt,pt,it,et");
    client.createCube(TEST_CUBE_NAME, cubeMeasures, cubeDimensions, cubeProperties);
    
    Set<String> measures = new HashSet<String>();
    measures.add("msr1");
    measures.add("msr2");
    measures.add("msr3");
    Set<String> dimensions = new HashSet<String>();
    dimensions.add("dim1");
    dimensions.add("dim2");
    client.createDerivedCube(TEST_CUBE_NAME, DERIVED_CUBE_NAME, measures,
        dimensions, new HashMap<String, String>(), 0L);
  }

  private void createCubeFact(CubeMetastoreClient client) throws HiveException {
    String factName = "testFact";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add dimensions of the cube
    factColumns.add(new FieldSchema("zipcode","int", "zip"));
    factColumns.add(new FieldSchema("cityid","int", "city id"));
    factColumns.add(new FieldSchema("ambigdim1", "string", "used in" +
        " testColumnAmbiguity"));

    Map<String, Set<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.MINUTELY);
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    updates.add(UpdatePeriod.MONTHLY);
    updates.add(UpdatePeriod.QUARTERLY);
    updates.add(UpdatePeriod.YEARLY);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);

    storageAggregatePeriods.put(c1, updates);
    storageAggregatePeriods.put(c2, updates);
    storageAggregatePeriods.put(c3, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s1);
    storageTables.put(c3, s1);
    // create cube fact
    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageAggregatePeriods, 0L, null, storageTables);
  }

  private void createCubeFactWeekly(CubeMetastoreClient client) throws HiveException {
    String factName = "testFactWeekly";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add dimensions of the cube
    factColumns.add(new FieldSchema("zipcode","int", "zip"));

    Map<String, Set<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.WEEKLY);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);

    storageAggregatePeriods.put(c1, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    // create cube fact
    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageAggregatePeriods, 0L, null, storageTables);
  }

  private void createCubeFactOnlyHourly(CubeMetastoreClient client)
      throws HiveException {
    String factName = "testFact2";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add dimensions of the cube
    factColumns.add(new FieldSchema("zipcode","int", "zip"));
    factColumns.add(new FieldSchema("cityid","int", "city id"));

    Map<String, Set<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);

    storageAggregatePeriods.put(c1, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    // create cube fact
    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageAggregatePeriods, 10L, null, storageTables);
    CubeFactTable fact2 = client.getFactTable(factName);
    // Add all hourly partitions for two days
    Calendar cal = Calendar.getInstance();
    cal.setTime(twodaysBack);
    Date temp = cal.getTime();
    while (!(temp.after(now))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), temp);
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact2.getName(),
          timeParts, null, UpdatePeriod.HOURLY);
      client.addPartition(sPartSpec, c1);
      cal.add(Calendar.HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }
  }

  private void createCubeFactOnlyHourlyRaw(CubeMetastoreClient client)
      throws HiveException {
    String factName = "testFact2_raw";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add dimensions of the cube
    factColumns.add(new FieldSchema("zipcode","int", "zip"));
    factColumns.add(new FieldSchema("cityid","int", "city id"));

    Map<String, Set<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    storageAggregatePeriods.put(c1, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    // create cube fact
    Map<String, String> properties = new HashMap<String, String>();
    properties.put(MetastoreConstants.FACT_AGGREGATED_PROPERTY, "false");

    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageAggregatePeriods, 100L, properties, storageTables);
    CubeFactTable fact2 = client.getFactTable(factName);
    // Add all hourly partitions for two days
    Calendar cal = Calendar.getInstance();
    cal.setTime(twodaysBack);
    Date temp = cal.getTime();
    while (!(temp.after(now))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), temp);
      StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact2.getName(),
          timeParts, null, UpdatePeriod.HOURLY);
      client.addPartition(sPartSpec, c1);
      cal.add(Calendar.HOUR_OF_DAY, 1);
      temp = cal.getTime();
    }
  }

  private void createCubeFactMonthly(CubeMetastoreClient client)
      throws HiveException {
    String factName = "testFactMonthly";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("countryid","int", "country id"));

    Map<String, Set<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.MONTHLY);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);

    storageAggregatePeriods.put(c2, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c2, s1);

    // create cube fact
    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageAggregatePeriods, 0L, null, storageTables);
  }

  //DimWithTwoStorages
  private void createCityTbale(CubeMetastoreClient client)
      throws HiveException {
    Set<CubeDimension> cityAttrs = new HashSet<CubeDimension>();
    cityAttrs.add(new BaseDimension(new FieldSchema("id", "int",
        "code")));
    cityAttrs.add(new BaseDimension(new FieldSchema("name", "string",
        "city name")));
    cityAttrs.add(new BaseDimension(new FieldSchema("ambigdim1", "string",
        "used in testColumnAmbiguity")));
    cityAttrs.add(new BaseDimension(new FieldSchema("ambigdim2", "string",
        "used in testColumnAmbiguity")));
    cityAttrs.add(new ReferencedDimension(
        new FieldSchema("stateid", "int", "state id"),
        new TableReference("statedim", "id")));
    cityAttrs.add(new ReferencedDimension(
        new FieldSchema("zipcode", "int", "zip code"),
        new TableReference("zipdim", "code")));
    UberDimension cityDim = new UberDimension("citydim", cityAttrs);
    client.createUberDimension(cityDim);

    String dimName = "citytable";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("stateid", "int", "state id"));
    dimColumns.add(new FieldSchema("zipcode", "int", "zip code"));
    dimColumns.add(new FieldSchema("ambigdim1", "string", "used in" +
        " testColumnAmbiguity"));
    dimColumns.add(new FieldSchema("ambigdim2", "string", "used in " +
        "testColumnAmbiguity"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c2, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    client.createCubeDimensionTable(cityDim.getName(), dimName, dimColumns, 0L,
         dumpPeriods, dimProps, storageTables);
  }


  private void createTestDim2(CubeMetastoreClient client)
      throws HiveException {
    String dimName = "testDim2";
    Set<CubeDimension> dimAttrs = new HashSet<CubeDimension>();
    dimAttrs.add(new BaseDimension(new FieldSchema("id", "int",
        "code")));
    dimAttrs.add(new BaseDimension(new FieldSchema("name", "string",
        "name")));
    dimAttrs.add(new ReferencedDimension(
        new FieldSchema("testDim3id", "string", "f-key to testdim3"),
        new TableReference("testdim3", "id")));
    UberDimension testDim2 = new UberDimension(dimName, dimAttrs);
    client.createUberDimension(testDim2);

    String dimTblName = "testDim2Tbl";
    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("testDim3id", "string", "f-key to testdim3"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c2, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L,
         dumpPeriods, dimProps, storageTables);
  }



  private void createTestDim3(CubeMetastoreClient client)
      throws HiveException {
    String dimName = "testDim3";

    Set<CubeDimension> dimAttrs = new HashSet<CubeDimension>();
    dimAttrs.add(new BaseDimension(new FieldSchema("id", "int",
        "code")));
    dimAttrs.add(new BaseDimension(new FieldSchema("name", "string",
        "name")));
    dimAttrs.add(new ReferencedDimension(
        new FieldSchema("testDim4id", "string", "f-key to testdim4"),
        new TableReference("testdim4", "id")));
    UberDimension testDim3 = new UberDimension(dimName, dimAttrs);
    client.createUberDimension(testDim3);

    String dimTblName = "testDim3Tbl";
    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("testDim4id", "string", "f-key to testDim4"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c2, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L,
         dumpPeriods, dimProps, storageTables);
  }

  private void createTestDim4(CubeMetastoreClient client)
      throws HiveException {
    String dimName = "testDim4";

    Set<CubeDimension> dimAttrs = new HashSet<CubeDimension>();
    dimAttrs.add(new BaseDimension(new FieldSchema("id", "int",
        "code")));
    dimAttrs.add(new BaseDimension(new FieldSchema("name", "string",
        "name")));
    UberDimension testDim4 = new UberDimension(dimName, dimAttrs);
    client.createUberDimension(testDim4);

    String dimTblName = "testDim4Tbl";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c2, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L,
         dumpPeriods, dimProps, storageTables);
  }

  private void createCyclicDim1(CubeMetastoreClient client)
      throws HiveException {
    String dimName = "cycleDim1";

    Set<CubeDimension> dimAttrs = new HashSet<CubeDimension>();
    dimAttrs.add(new BaseDimension(new FieldSchema("id", "int",
        "code")));
    dimAttrs.add(new BaseDimension(new FieldSchema("name", "string",
        "name")));
    dimAttrs.add(new ReferencedDimension(
        new FieldSchema("cyleDim2Id", "string", "link to cyclic dim 2"),
        new TableReference("cycleDim2", "id")));
    UberDimension cycleDim1 = new UberDimension(dimName, dimAttrs);
    client.createUberDimension(cycleDim1);

    String dimTblName = "cycleDim1Tbl";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("cyleDim2Id", "string", "link to cyclic dim 2"));

    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();
    dimensionReferences.put("cyleDim2Id", Arrays.asList(new TableReference("cycleDim2", "id")));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c2, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L,
         dumpPeriods, dimProps, storageTables);
  }

  private void createCyclicDim2(CubeMetastoreClient client)
      throws HiveException {
    String dimName = "cycleDim2";

    Set<CubeDimension> dimAttrs = new HashSet<CubeDimension>();
    dimAttrs.add(new BaseDimension(new FieldSchema("id", "int",
        "code")));
    dimAttrs.add(new BaseDimension(new FieldSchema("name", "string",
        "name")));
    dimAttrs.add(new ReferencedDimension(
        new FieldSchema("cyleDim1Id", "string", "link to cyclic dim 1"),
        new TableReference("cycleDim1", "id")));
    UberDimension cycleDim2 = new UberDimension(dimName, dimAttrs);
    client.createUberDimension(cycleDim2);

    String dimTblName = "cycleDim2Tbl";
    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("cyleDim1Id", "string", "link to cyclic dim 1"));

    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();
    dimensionReferences.put("cyleDim1Id", Arrays.asList(new TableReference("cycleDim1", "id")));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c2, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L,
         dumpPeriods, dimProps, storageTables);
  }

  private void createZiptable(CubeMetastoreClient client) throws Exception {
    String dimName = "zipdim";

    Set<CubeDimension> dimAttrs = new HashSet<CubeDimension>();
    dimAttrs.add(new BaseDimension(new FieldSchema("code", "int",
        "code")));
    dimAttrs.add(new BaseDimension(new FieldSchema("f1", "string",
        "name")));
    dimAttrs.add(new BaseDimension(new FieldSchema("f2", "string",
        "name")));
    UberDimension zipDim = new UberDimension(dimName, dimAttrs);
    client.createUberDimension(zipDim);

    String dimTblName = "ziptable";
    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("code", "int", "code"));
    dimColumns.add(new FieldSchema("f1", "string", "field1"));
    dimColumns.add(new FieldSchema("f2", "string", "field2"));

    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();
    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L,
         dumpPeriods, dimProps, storageTables);
  }

  private void createCountryTable(CubeMetastoreClient client) throws Exception {
    String dimName = "countrydim";

    Set<CubeDimension> dimAttrs = new HashSet<CubeDimension>();
    dimAttrs.add(new BaseDimension(new FieldSchema("id", "int",
        "code")));
    dimAttrs.add(new BaseDimension(new FieldSchema("name", "string",
        "name")));
    dimAttrs.add(new BaseDimension(new FieldSchema("captial", "string",
        "field2")));
    dimAttrs.add(new BaseDimension(new FieldSchema("region", "string",
        "region name")));
    dimAttrs.add(new BaseDimension(new FieldSchema("ambigdim2", "string",
        "used in testColumnAmbiguity")));
    UberDimension countryDim = new UberDimension(dimName, dimAttrs);
    client.createUberDimension(countryDim);

    String dimTblName = "countrytable";
    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("capital", "string", "field2"));
    dimColumns.add(new FieldSchema("region", "string", "region name"));
    dimColumns.add(new FieldSchema("ambigdim2", "string", "used in" +
        " testColumnAmbiguity"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    dumpPeriods.put(c1, null);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L,
         dumpPeriods, dimProps, storageTables);
  }

  private void createStateTable(CubeMetastoreClient client) throws Exception {
    String dimName = "statedim";

    Set<CubeDimension> dimAttrs = new HashSet<CubeDimension>();
    dimAttrs.add(new BaseDimension(new FieldSchema("id", "int",
        "code")));
    dimAttrs.add(new BaseDimension(new FieldSchema("name", "string",
        "name")));
    dimAttrs.add(new BaseDimension(new FieldSchema("capital", "string",
        "field2")));
    dimAttrs.add(new ReferencedDimension(
        new FieldSchema("countryid", "string", "link to country table"),
        new TableReference("countrydim", "id")));
    UberDimension countryDim = new UberDimension(dimName, dimAttrs);
    client.createUberDimension(countryDim);

    String dimTblName = "statetable";
    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("capital", "string", "field2"));
    dimColumns.add(new FieldSchema("countryid", "string", "region name"));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    client.createCubeDimensionTable(dimName, dimTblName, dimColumns, 0L,
         dumpPeriods, dimProps, storageTables);
  }

  public void createSources(HiveConf conf, String dbName) throws Exception {
    CubeMetastoreClient client =  CubeMetastoreClient.getInstance(conf);
    Database database = new Database();
    database.setName(dbName);
    Hive.get(conf).createDatabase(database);
    client.setCurrentDatabase(dbName);
    client.createStorage(new HDFSStorage(c1));
    client.createStorage(new HDFSStorage(c2));
    client.createStorage(new HDFSStorage(c3));
    createCube(client);
    createCubeFact(client);
    // commenting this as the week date format throws IllegalPatternException
    //createCubeFactWeekly(client);
    createCubeFactOnlyHourly(client);
    createCubeFactOnlyHourlyRaw(client);

    createCityTbale(client);
    // For join resolver test
    createTestDim2(client);
    createTestDim3(client);
    createTestDim4(client);

    // For join resolver cyclic links in dimension tables
    createCyclicDim1(client);
    createCyclicDim2(client);

    createCubeFactMonthly(client);
    createZiptable(client);
    createCountryTable(client);
    createStateTable(client);
    createCubeFactsWithValidColumns(client);
  }

  public void dropSources(HiveConf conf) throws Exception {
    Hive metastore = Hive.get(conf);
    metastore.dropDatabase(SessionState.get().getCurrentDatabase(), true, true, true);
  }

  private void createCubeFactsWithValidColumns(CubeMetastoreClient client)
      throws HiveException {
    String factName = "summary1";
    StringBuilder commonCols = new StringBuilder();
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
      commonCols.append(measure.getName());
      commonCols.append(",");
    }

    // add dimensions of the cube
    factColumns.add(new FieldSchema("dim1","string", "dim1"));
    factColumns.add(new FieldSchema("dim2","string", "dim2"));
    factColumns.add(new FieldSchema("zipcode","int", "zip"));
    factColumns.add(new FieldSchema("cityid","int", "city id"));
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.MINUTELY);
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    Map<String, Set<UpdatePeriod>> storageUpdatePeriods =
        new HashMap<String, Set<UpdatePeriod>>();

    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(TestCubeMetastoreClient.getDatePartition());
    timePartCols.add(TestCubeMetastoreClient.getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);

    ArrayList<FieldSchema> partCols2 = new ArrayList<FieldSchema>();
    List<String> timePartCols2 = new ArrayList<String>();
    partCols2.add(new FieldSchema("pt", "string", "p time"));
    partCols2.add(new FieldSchema("it", "string", "i time"));
    partCols2.add(new FieldSchema("et", "string", "e time"));
    timePartCols2.add("pt");
    timePartCols2.add("it");
    timePartCols2.add("et");
    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s2.setPartCols(partCols2);
    s2.setTimePartCols(timePartCols2);

    storageUpdatePeriods.put(c1, updates);
    storageUpdatePeriods.put(c2, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    // create cube fact summary1
    Map<String, String> properties = new HashMap<String, String>();
    String validColumns = commonCols.toString() + ",dim1";
    properties.put(MetastoreUtil.getValidColumnsKey(factName),
        validColumns);
    CubeFactTable fact1 = new CubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageUpdatePeriods, 10L, properties);
    client.createCubeTable(fact1, storageTables);
    createPIEParts(client, fact1, c2);

    // create summary2 - same schema, different valid columns
    factName = "summary2";
    properties = new HashMap<String, String>();
    validColumns = commonCols.toString() + ",dim1,dim2";
    properties.put(MetastoreUtil.getValidColumnsKey(factName),
        validColumns);
    CubeFactTable fact2 = new CubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageUpdatePeriods, 20L, properties);
    client.createCubeTable(fact2, storageTables);
    createPIEParts(client, fact2, c2);

    factName = "summary3";
    properties = new HashMap<String, String>();
    validColumns = commonCols.toString() + ",dim1,dim2,cityid";
    properties.put(MetastoreUtil.getValidColumnsKey(factName),
        validColumns);
    CubeFactTable fact3 = new CubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageUpdatePeriods, 30L, properties);
    client.createCubeTable(fact3, storageTables);
    createPIEParts(client, fact3, c2);
  }

  private void createPIEParts(CubeMetastoreClient client, CubeFactTable fact,
      String storageName) throws HiveException {
    // Add partitions in PIE storage
    Calendar pcal = Calendar.getInstance();
    pcal.setTime(twodaysBack);
    pcal.set(Calendar.HOUR, 0);
    Calendar ical = Calendar.getInstance();
    ical.setTime(twodaysBack);
    ical.set(Calendar.HOUR, 0);
    // pt=day1 and it=day1
    // pt=day2-hour[0-3] it = day1-hour[20-23]
    // pt=day2 and it=day1
    // pt=day2-hour[4-23] it = day2-hour[0-19]
    // pt=day2 and it=day2
    // pt=day3-hour[0-3] it = day2-hour[20-23]
    // pt=day3-hour[4-23] it = day3-hour[0-19]
    for (int p = 1; p <= 3; p++) {
      Date ptime = pcal.getTime();
      Date itime = ical.getTime();
      Map<String, Date> timeParts = new HashMap<String, Date>();
      if (p == 1) { // day1
        timeParts.put("pt", ptime);
        timeParts.put("it", itime);
        timeParts.put("et", itime);
        StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(),
            timeParts, null, UpdatePeriod.DAILY);
        client.addPartition(sPartSpec, storageName);
        pcal.add(Calendar.DAY_OF_MONTH, 1);
        ical.add(Calendar.HOUR_OF_DAY, 20);
      } else if (p == 2) { // day2
        // pt=day2-hour[0-3] it = day1-hour[20-23]
        // pt=day2 and it=day1
        // pt=day2-hour[4-23] it = day2-hour[0-19]
        // pt=day2 and it=day2
        ptime = pcal.getTime();
        itime = ical.getTime();
        timeParts.put("pt", ptime);
        timeParts.put("it", itime);
        timeParts.put("et", itime);
        // pt=day2 and it=day1
        StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(),
            timeParts, null, UpdatePeriod.DAILY);
        client.addPartition(sPartSpec, storageName);
        // pt=day2-hour[0-3] it = day1-hour[20-23]
        // pt=day2-hour[4-23] it = day2-hour[0-19]
        for (int i = 0; i < 24; i++) {
          ptime = pcal.getTime();
          itime = ical.getTime();
          timeParts.put("pt", ptime);
          timeParts.put("it", itime);
          timeParts.put("et", itime);
          sPartSpec = new StoragePartitionDesc(fact.getName(),
              timeParts, null, UpdatePeriod.HOURLY);
          client.addPartition(sPartSpec, storageName);
          pcal.add(Calendar.HOUR_OF_DAY, 1);
          ical.add(Calendar.HOUR_OF_DAY, 1);
        }
        // pt=day2 and it=day2
        sPartSpec = new StoragePartitionDesc(fact.getName(),
            timeParts, null, UpdatePeriod.DAILY);
        client.addPartition(sPartSpec, storageName);
      }
      else if (p == 3) { // day3
        // pt=day3-hour[0-3] it = day2-hour[20-23]
        // pt=day3-hour[4-23] it = day3-hour[0-19]
        for (int i = 0; i < 24; i++) {
          ptime = pcal.getTime();
          itime = ical.getTime();
          timeParts.put("pt", ptime);
          timeParts.put("it", itime);
          timeParts.put("et", itime);
          StoragePartitionDesc sPartSpec = new StoragePartitionDesc(fact.getName(),
              timeParts, null, UpdatePeriod.HOURLY);
          client.addPartition(sPartSpec, storageName);
          pcal.add(Calendar.HOUR_OF_DAY, 1);
          ical.add(Calendar.HOUR_OF_DAY, 1);
        }
      }
    }
  }

  public static void  printQueryAST(String query, String label)
      throws ParseException {
    System.out.println("--" + label + "--AST--");
    System.out.println("--query- " + query);
    HQLParser.printAST(HQLParser.parseHQL(query));
  }

}
