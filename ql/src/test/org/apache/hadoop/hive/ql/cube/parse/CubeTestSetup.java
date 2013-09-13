package org.apache.hadoop.hive.ql.cube.parse;

import static org.junit.Assert.assertEquals;

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
import org.apache.hadoop.hive.ql.cube.metadata.MetastoreUtil;
import org.apache.hadoop.hive.ql.cube.metadata.ReferencedDimension;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.TableReference;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
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

  public static final String cubeName = "testcube";
  public static String HOUR_FMT = "yyyy-MM-dd HH";
  public static final SimpleDateFormat HOUR_PARSER = new SimpleDateFormat(
    HOUR_FMT);
  public static String MONTH_FMT = "yyyy-MM";
  public static final SimpleDateFormat MONTH_PARSER = new SimpleDateFormat(
    MONTH_FMT);
  private Set<CubeMeasure> cubeMeasures;
  private Set<CubeDimension> cubeDimensions;
  public static final String TEST_CUBE_NAME= "testCube";
  public static Date now;
  public static Date twodaysBack;
  public static String twoDaysRange;
  public static Date twoMonthsBack;
  public static String twoMonthsRangeUptoMonth;
  public static String twoMonthsRangeUptoHours;
  private static boolean zerothHour;

  public static void init () {
    Calendar cal = Calendar.getInstance();
    now = cal.getTime();
    zerothHour = (cal.get(Calendar.HOUR_OF_DAY) == 0);
    System.out.println("Test now:" + now);
    cal.add(Calendar.DAY_OF_MONTH, -2);
    twodaysBack = cal.getTime();
    System.out.println("Test twodaysBack:" + twodaysBack);
    cal = Calendar.getInstance();
    cal.add(Calendar.MONTH, -2);
    twoMonthsBack = cal.getTime();
    System.out.println("Test twoMonthsBack:" + twoMonthsBack);

    twoDaysRange = "time_range_in('dt', '" + getDateUptoHours(
      twodaysBack) + "','" + getDateUptoHours(now) + "')";
    twoMonthsRangeUptoMonth = "time_range_in('dt', '" +
      getDateUptoMonth(twoMonthsBack) + "','" + getDateUptoMonth(now) + "')";
    twoMonthsRangeUptoHours = "time_range_in('dt', '" +
      getDateUptoHours(twoMonthsBack) + "','" + getDateUptoHours(now) + "')";
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
      expected.append(storageTable);
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
    storageTableToWhereClause.put(StringUtils.join(storageTables, ","),
        whereClause);
    return storageTableToWhereClause;
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
    boolean hourlyPartsAvailable = false;
    if (!CubeTestSetup.isZerothHour()) {
      addParts(hourlyparts, UpdatePeriod.HOURLY, twoMonthsBack,
        DateUtil.getCeilDate(twoMonthsBack, UpdatePeriod.DAILY));
      addParts(hourlyparts, UpdatePeriod.HOURLY,
        DateUtil.getFloorDate(now, UpdatePeriod.DAILY),
        DateUtil.getFloorDate(now, UpdatePeriod.HOURLY));
      dayStart = DateUtil.getCeilDate(
        twoMonthsBack, UpdatePeriod.DAILY);
      monthStart = DateUtil.getCeilDate(twoMonthsBack, UpdatePeriod.MONTHLY);
      hourlyPartsAvailable = true;
    }
    Calendar cal = new GregorianCalendar();
    cal.setTime(dayStart);
    boolean dailyPartsAvailable = false;
    if (cal.get(Calendar.DAY_OF_MONTH) != 1) {
      addParts(dailyparts, UpdatePeriod.DAILY, dayStart,
        DateUtil.getCeilDate(twoMonthsBack, UpdatePeriod.MONTHLY));
      monthStart = DateUtil.getCeilDate(twoMonthsBack, UpdatePeriod.MONTHLY);
      dailyPartsAvailable = true;
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
      if (hourlyPartsAvailable) {
        tables.append(storageTables[0]);
        tables.append(",");
      }
      if (dailyPartsAvailable) {
        tables.append(storageTables[1]);
        tables.append(",");
      }
      tables.append(storageTables[2]);
    } else {
      tables.append(storageTables[0]);
    }
    Collections.sort(parts);
    storageTableToWhereClause.put(tables.toString(),
      StorageUtil.getWherePartClause("dt", cubeName, parts));
    return storageTableToWhereClause;
  }

  public static Map<String, String> getWhereForMonthly2months(String monthlyTable) {
    Map<String, String> storageTableToWhereClause =
      new LinkedHashMap<String, String>();
    List<String> parts = new ArrayList<String>();
    addParts(parts, UpdatePeriod.MONTHLY,
      twoMonthsBack,
      DateUtil.getFloorDate(now, UpdatePeriod.MONTHLY));
    storageTableToWhereClause.put(monthlyTable,
      StorageUtil.getWherePartClause("dt", cubeName, parts));
    return storageTableToWhereClause;
  }

  public static Map<String, String> getWhereForHourly2days(String hourlyTable) {
    Map<String, String> storageTableToWhereClause =
      new LinkedHashMap<String, String>();
    List<String> parts = new ArrayList<String>();
    addParts(parts, UpdatePeriod.HOURLY, twodaysBack,
      DateUtil.getFloorDate(now, UpdatePeriod.HOURLY));
    storageTableToWhereClause.put(hourlyTable,
      StorageUtil.getWherePartClause("dt", cubeName, parts));
    return storageTableToWhereClause;
  }

  public static void addParts(List<String> partitions, UpdatePeriod updatePeriod,
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

  public static String rewrite(CubeQueryRewriter driver, String query)
    throws SemanticException, ParseException {
    CubeQueryContext rewrittenQuery = driver.rewrite(query);
    return rewrittenQuery.toHQL();
  }

  public static String getExpectedQuery(String dimName, String selExpr, String postWhereExpr,
                          String storageTable, boolean hasPart) {
    StringBuilder expected = new StringBuilder();
    expected.append(selExpr);
    expected.append(storageTable);
    expected.append(" ");
    expected.append(dimName);
    if (hasPart) {
      expected.append(" WHERE ");
      expected.append(StorageUtil.getWherePartClause("dt",
        dimName, Storage.getPartitionsForLatest()));
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
        "int", "zip"), new TableReference("ziptable", "zipcode")));
    locationHierarchy.add(new ReferencedDimension(new FieldSchema("cityid",
        "int", "city"), new TableReference("citytable", "id")));
    locationHierarchy.add(new ReferencedDimension(new FieldSchema("stateid",
        "int", "state"), new TableReference("statetable", "id")));
    locationHierarchy.add(new ReferencedDimension(new FieldSchema("countryid",
        "int", "country"), new TableReference("countrytable", "id")));
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

    Map<Storage, Set<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.MINUTELY);
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    updates.add(UpdatePeriod.MONTHLY);
    updates.add(UpdatePeriod.QUARTERLY);
    updates.add(UpdatePeriod.YEARLY);
    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage.addToPartCols(Storage.getDatePartition());
    storageAggregatePeriods.put(hdfsStorage, updates);
    Storage hdfsStorage2 = new HDFSStorage("C2",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage2.addToPartCols(Storage.getDatePartition());
    storageAggregatePeriods.put(hdfsStorage2, updates);
    Storage hdfsStorage3 = new HDFSStorage("C3",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage3.addToPartCols(Storage.getDatePartition());
    storageAggregatePeriods.put(hdfsStorage3, updates);

    // create cube fact
    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageAggregatePeriods, 0L, null);
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

    Map<Storage, Set<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.WEEKLY);
    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage.addToPartCols(Storage.getDatePartition());
    storageAggregatePeriods.put(hdfsStorage, updates);

    // create cube fact
    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageAggregatePeriods, 0L, null);
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

    Map<Storage, Set<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage.addToPartCols(Storage.getDatePartition());
    storageAggregatePeriods.put(hdfsStorage, updates);

    // create cube fact
    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageAggregatePeriods, 10L, null);
    CubeFactTable fact2 = client.getFactTable(factName);
    // Add all hourly partitions for two days
    Calendar cal = Calendar.getInstance();
    cal.setTime(twodaysBack);
    Date temp = cal.getTime();
    while (!(temp.after(now))) {
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put(Storage.getDatePartitionKey(), temp);
      client.addPartition(fact2, hdfsStorage,
        UpdatePeriod.HOURLY, timeParts);
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

    Map<Storage, Set<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.MONTHLY);
    Storage hdfsStorage = new HDFSStorage("C2",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage.addToPartCols(Storage.getDatePartition());
    storageAggregatePeriods.put(hdfsStorage, updates);

    // create cube fact
    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageAggregatePeriods, 0L, null);
  }

  //DimWithTwoStorages
  private void createCityTbale(CubeMetastoreClient client)
      throws HiveException {
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
    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();
    dimensionReferences.put("stateid", Arrays.asList(new TableReference("statetable", "id")));
    dimensionReferences.put("zipcode", Arrays.asList(new TableReference("ziptable", "code")));

    Storage hdfsStorage1 = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Storage hdfsStorage2 = new HDFSStorage("C2",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Map<Storage, UpdatePeriod> snapshotDumpPeriods =
        new HashMap<Storage, UpdatePeriod>();
    snapshotDumpPeriods.put(hdfsStorage1, UpdatePeriod.HOURLY);
    snapshotDumpPeriods.put(hdfsStorage2, null);
    client.createCubeDimensionTable(dimName, dimColumns, 0L,
        dimensionReferences, snapshotDumpPeriods, null);
  }


  private void createTestDim2(CubeMetastoreClient client)
      throws HiveException {
    String dimName = "testDim2";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("tetDim3id", "string", "f-key to testdim3"));

    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();

    dimensionReferences.put("testDim3id", Arrays.asList(new TableReference("testdim3", "id")));

    Storage hdfsStorage1 = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Storage hdfsStorage2 = new HDFSStorage("C2",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Map<Storage, UpdatePeriod> snapshotDumpPeriods =
        new HashMap<Storage, UpdatePeriod>();
    snapshotDumpPeriods.put(hdfsStorage1, UpdatePeriod.HOURLY);
    snapshotDumpPeriods.put(hdfsStorage2, null);

    client.createCubeDimensionTable(dimName, dimColumns, 0L,
        dimensionReferences, snapshotDumpPeriods, null);
  }



  private void createTestDim3(CubeMetastoreClient client)
      throws HiveException {
    String dimName = "testDim3";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("testDim4id", "string", "f-key to testDim4"));

    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();
    dimensionReferences.put("testDim4id", Arrays.asList(new TableReference("testdim4", "id")));

    Storage hdfsStorage1 = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Storage hdfsStorage2 = new HDFSStorage("C2",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Map<Storage, UpdatePeriod> snapshotDumpPeriods =
        new HashMap<Storage, UpdatePeriod>();
    snapshotDumpPeriods.put(hdfsStorage1, UpdatePeriod.HOURLY);
    snapshotDumpPeriods.put(hdfsStorage2, null);

    client.createCubeDimensionTable(dimName, dimColumns, 0L,
        dimensionReferences, snapshotDumpPeriods, null);
  }

  private void createTestDim4(CubeMetastoreClient client)
      throws HiveException {
    String dimName = "testDim4";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));

    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();

    Storage hdfsStorage1 = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Storage hdfsStorage2 = new HDFSStorage("C2",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Map<Storage, UpdatePeriod> snapshotDumpPeriods =
        new HashMap<Storage, UpdatePeriod>();
    snapshotDumpPeriods.put(hdfsStorage1, UpdatePeriod.HOURLY);
    snapshotDumpPeriods.put(hdfsStorage2, null);

    client.createCubeDimensionTable(dimName, dimColumns, 0L,
        dimensionReferences, snapshotDumpPeriods, null);
  }


  private void createZiptable(CubeMetastoreClient client) throws Exception {
    String dimName = "ziptable";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("code", "int", "code"));
    dimColumns.add(new FieldSchema("f1", "string", "field1"));
    dimColumns.add(new FieldSchema("f2", "string", "field2"));

    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();
    Map<Storage, UpdatePeriod> snapshotDumpPeriods =
        new HashMap<Storage, UpdatePeriod>();
    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    snapshotDumpPeriods.put(hdfsStorage, UpdatePeriod.HOURLY);
    dumpPeriods.put(hdfsStorage.getName(), UpdatePeriod.HOURLY);
    client.createCubeDimensionTable(dimName, dimColumns, 0L,
        dimensionReferences, snapshotDumpPeriods, null);
  }

  private void createCountryTable(CubeMetastoreClient client) throws Exception {
    String dimName = "countrytable";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("capital", "string", "field2"));
    dimColumns.add(new FieldSchema("region", "string", "region name"));
    dimColumns.add(new FieldSchema("ambigdim2", "string", "used in" +
        " testColumnAmbiguity"));
    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();
    Map<Storage, UpdatePeriod> snapshotDumpPeriods =
        new HashMap<Storage, UpdatePeriod>();
    snapshotDumpPeriods.put(hdfsStorage, null);
    client.createCubeDimensionTable(dimName, dimColumns, 0L,
        dimensionReferences, snapshotDumpPeriods, null);
  }

  private void createStateTable(CubeMetastoreClient client) throws Exception {
    String dimName = "statetable";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("capital", "string", "field2"));
    dimColumns.add(new FieldSchema("countryid", "string", "region name"));

    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();
    dimensionReferences.put("countryid", Arrays.asList(new TableReference("countrytable",
        "id")));

    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Map<Storage, UpdatePeriod> snapshotDumpPeriods =
        new HashMap<Storage, UpdatePeriod>();
    snapshotDumpPeriods.put(hdfsStorage, UpdatePeriod.HOURLY);
    client.createCubeDimensionTable(dimName, dimColumns, 0L,
        dimensionReferences, snapshotDumpPeriods, null);
  }

  public void createSources(HiveConf conf, String dbName) throws Exception {
    CubeMetastoreClient client =  CubeMetastoreClient.getInstance(conf);
    Database database = new Database();
    database.setName(dbName);
    Hive.get(conf).createDatabase(database);
    client.setCurrentDatabase(dbName);
    createCube(client);
    createCubeFact(client);
    // commenting this as the week date format throws IllegalPatternException
    //createCubeFactWeekly(client);
    createCubeFactOnlyHourly(client);
    createCityTbale(client);
    // For join resolver test
    createTestDim2(client);
    createTestDim3(client);
    createTestDim4(client);

    createCubeFactMonthly(client);
    createZiptable(client);
    createCountryTable(client);
    createStateTable(client);
    createCubeFactsWithValidColumns(client);
  }

  public void dropSources(HiveConf conf) throws Exception {
    Hive metastore = Hive.get(conf);
    metastore.dropDatabase(metastore.getCurrentDatabase(), true, true, true);
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

    Set<Storage> storages = new HashSet<Storage>();
    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage.addToPartCols(Storage.getDatePartition());
    storages.add(hdfsStorage);
    storageUpdatePeriods.put(hdfsStorage.getName(), updates);
    Storage hdfsStorage2 = new HDFSStorage("C2",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage2.addToPartCols(new FieldSchema("pt", "string", "p time"));
    hdfsStorage2.addToPartCols(new FieldSchema("it", "string", "i time"));
    hdfsStorage2.addToPartCols(new FieldSchema("et", "string", "e time"));
    storages.add(hdfsStorage2);
    storageUpdatePeriods.put(hdfsStorage2.getName(), updates);

    // create cube fact summary1
    Map<String, String> properties = new HashMap<String, String>();
    String validColumns = commonCols.toString() + ",dim1";
    properties.put(MetastoreUtil.getValidColumnsKey(factName),
        validColumns);
    CubeFactTable fact1 = new CubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageUpdatePeriods, 10L, properties);
    client.createCubeTable(fact1, storages);
    createPIEParts(client, fact1, hdfsStorage2);

    // create summary2 - same schema, different valid columns
    factName = "summary2";
    storages = new HashSet<Storage>();
    hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage.addToPartCols(Storage.getDatePartition());
    storages.add(hdfsStorage);
    storageUpdatePeriods.put(hdfsStorage.getName(), updates);
    hdfsStorage2 = new HDFSStorage("C2",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage2.addToPartCols(new FieldSchema("pt", "string", "p time"));
    hdfsStorage2.addToPartCols(new FieldSchema("it", "string", "i time"));
    hdfsStorage2.addToPartCols(new FieldSchema("et", "string", "e time"));
    storages.add(hdfsStorage2);
    storageUpdatePeriods.put(hdfsStorage2.getName(), updates);
    properties = new HashMap<String, String>();
    validColumns = commonCols.toString() + ",dim1,dim2";
    properties.put(MetastoreUtil.getValidColumnsKey(factName),
        validColumns);
    CubeFactTable fact2 = new CubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageUpdatePeriods, 20L, properties);
    client.createCubeTable(fact2, storages);
    createPIEParts(client, fact2, hdfsStorage2);

    factName = "summary3";
    storages = new HashSet<Storage>();
    hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage.addToPartCols(Storage.getDatePartition());
    storages.add(hdfsStorage);
    storageUpdatePeriods.put(hdfsStorage.getName(), updates);
    hdfsStorage2 = new HDFSStorage("C2",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage2.addToPartCols(new FieldSchema("pt", "string", "p time"));
    hdfsStorage2.addToPartCols(new FieldSchema("it", "string", "i time"));
    hdfsStorage2.addToPartCols(new FieldSchema("et", "string", "e time"));
    storages.add(hdfsStorage2);
    storageUpdatePeriods.put(hdfsStorage2.getName(), updates);
    properties = new HashMap<String, String>();
    validColumns = commonCols.toString() + ",dim1,dim2,cityid";
    properties.put(MetastoreUtil.getValidColumnsKey(factName),
        validColumns);
    CubeFactTable fact3 = new CubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageUpdatePeriods, 30L, properties);
    client.createCubeTable(fact3, storages);
    createPIEParts(client, fact3, hdfsStorage2);
  }

  private void createPIEParts(CubeMetastoreClient client, CubeFactTable fact,
      Storage hdfsStorage) throws HiveException {
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
        client.addPartition(fact, hdfsStorage,
            UpdatePeriod.DAILY, timeParts);
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
        client.addPartition(fact, hdfsStorage,
            UpdatePeriod.DAILY, timeParts);
        // pt=day2-hour[0-3] it = day1-hour[20-23]
        // pt=day2-hour[4-23] it = day2-hour[0-19]
        for (int i = 0; i < 24; i++) {
          ptime = pcal.getTime();
          itime = ical.getTime();
          timeParts.put("pt", ptime);
          timeParts.put("it", itime);
          timeParts.put("et", itime);
          client.addPartition(fact, hdfsStorage,
              UpdatePeriod.HOURLY, timeParts);
          pcal.add(Calendar.HOUR_OF_DAY, 1);
          ical.add(Calendar.HOUR_OF_DAY, 1);
        }
        // pt=day2 and it=day2
        client.addPartition(fact, hdfsStorage,
            UpdatePeriod.DAILY, timeParts);
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
          client.addPartition(fact, hdfsStorage,
              UpdatePeriod.HOURLY, timeParts);
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
