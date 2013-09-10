package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.mapred.SequenceFileInputFormat;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCubeMetastoreClient {

  private static CubeMetastoreClient client;

  //cube members
  private static Cube cube;
  private static Cube cubeWithProps;
  private static Set<CubeMeasure> cubeMeasures;
  private static Set<CubeDimension> cubeDimensions;
  private static final String cubeName = "testMetastoreCube";
  private static final String cubeNameWithProps = "testMetastoreCubeWithProps";
  private static final Map<String, String> cubeProperties =
      new HashMap<String, String>();
  private static Date now;
  private static HiveConf conf = new HiveConf(TestCubeMetastoreClient.class);

  @BeforeClass
  public static void setup() throws HiveException, AlreadyExistsException {
    client =  CubeMetastoreClient.getInstance(conf);
    now = new Date();
    Database database = new Database();
    database.setName(TestCubeMetastoreClient.class.getSimpleName());
    Hive.get(conf).createDatabase(database);
    client.setCurrentDatabase(TestCubeMetastoreClient.class.getSimpleName());
    defineCube();
  }

  @AfterClass
  public static void teardown() throws Exception {
    Hive.get().dropDatabase(TestCubeMetastoreClient.class.getSimpleName(),
        true, true, true);
    CubeMetastoreClient.close();
  }

  private static void defineCube() {
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
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msrstarttime", "int",
        "measure with start time"), null, null, null, now, null, null));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msrendtime", "float",
        "measure with end time"), null, "SUM", "RS", now, now, null));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msrcost", "double",
        "measure with cost"), null, "MAX", null, now, now, 100.0));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msrcost2", "bigint",
        "measure with cost"), null, "MAX", null, null, null, 100.0));
    cubeMeasures.add(new ExprMeasure(new FieldSchema("msr5start", "double",
        "expr measure with start and end times"),
        "avg(msr1 + msr2)", null, null, null, now, now, 100.0));
    cubeMeasures.add(new ExprMeasure(new FieldSchema("msrexpr", "bigint",
        "expr measure with all fields"),
        "(msr1 + msr2)/ msr4", "", "SUM", "RS", now, now, 100.0));

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
    cubeDimensions.add(new ReferencedDimension(
        new FieldSchema("dim2", "string", "ref dim"),
        new TableReference("testdim2", "id")));

    List<CubeDimension> locationHierarchyWithStartTime = new ArrayList<CubeDimension>();
    locationHierarchyWithStartTime.add(new ReferencedDimension(
        new FieldSchema("zipcode2","int", "zip"),
        new TableReference("ziptable", "zipcode"), now, now, 100.0));
    locationHierarchyWithStartTime.add(new ReferencedDimension(
        new FieldSchema("cityid2", "int", "city"),
        new TableReference("citytable", "id"), now, null, null));
    locationHierarchyWithStartTime.add(new ReferencedDimension(
        new FieldSchema("stateid2", "int", "state"),
        new TableReference("statetable", "id"), now, null, 100.0));
    locationHierarchyWithStartTime.add(
        new ReferencedDimension(new FieldSchema("countryid2", "int", "country"),
        new TableReference("countrytable", "id"),
        null, null, null));
    locationHierarchyWithStartTime.add(new InlineDimension(
        new FieldSchema("regionname2","string", "region"), regions));

    cubeDimensions.add(new HierarchicalDimension("location2",
        locationHierarchyWithStartTime));
    cubeDimensions.add(new BaseDimension(new FieldSchema("dim1startTime", "string",
        "basedim"), now, null, 100.0));
    cubeDimensions.add(new ReferencedDimension(
        new FieldSchema("dim2start", "string", "ref dim"),
        new TableReference("testdim2", "id"), now, now, 100.0));

    List<TableReference> multiRefs = new ArrayList<TableReference>();
    multiRefs.add(new TableReference("testdim2", "id"));
    multiRefs.add(new TableReference("testdim3", "id"));
    multiRefs.add(new TableReference("testdim4", "id"));

    cubeDimensions.add(
        new ReferencedDimension(new FieldSchema("dim3", "string",
            "multi ref dim"), multiRefs));
    cubeDimensions.add(
        new ReferencedDimension(new FieldSchema("dim3start", "string",
            "multi ref dim"), multiRefs, now, null, 100.0));


    cubeDimensions.add(new InlineDimension(
        new FieldSchema("region", "string", "region dim"), regions));
    cubeDimensions.add(new InlineDimension(
        new FieldSchema("regionstart", "string", "region dim"), now,
        null, 100.0, regions));
    cube = new Cube(cubeName, cubeMeasures, cubeDimensions);

    cubeProperties.put(MetastoreUtil.getCubeTimedDimensionListKey(
        cubeNameWithProps), "dt,mydate");
    cubeWithProps = new Cube(cubeNameWithProps, cubeMeasures, cubeDimensions,
        cubeProperties);
  }

  @Test
  public void testCube() throws Exception {
    Assert.assertEquals(client.getCurrentDatabase(),
        this.getClass().getSimpleName());
    client.createCube(cubeName, cubeMeasures, cubeDimensions);
    Assert.assertTrue(client.tableExists(cubeName));
    Table cubeTbl = client.getHiveTable(cubeName);
    Assert.assertTrue(client.isCube(cubeTbl));
    Cube cube2 = new Cube(cubeTbl);
    Assert.assertTrue(cube.equals(cube2));
    Assert.assertNull(cube.getTimedDimensions());

    client.createCube(cubeNameWithProps, cubeMeasures, cubeDimensions,
        cubeProperties);
    Assert.assertTrue(client.tableExists(cubeNameWithProps));
    cubeTbl = client.getHiveTable(cubeNameWithProps);
    Assert.assertTrue(client.isCube(cubeTbl));
    cube2 = new Cube(cubeTbl);
    Assert.assertTrue(cubeWithProps.equals(cube2));
    Assert.assertNotNull(cubeWithProps.getTimedDimensions());
    Assert.assertTrue(cubeWithProps.getTimedDimensions().contains("dt"));
    Assert.assertTrue(cubeWithProps.getTimedDimensions().contains("mydate"));
  }

  @Test
  public void testCubeFact() throws Exception {
    String factName = "testMetastoreFact";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode","int", "zip"));

    Map<String, Set<UpdatePeriod>> updatePeriods =
        new HashMap<String, Set<UpdatePeriod>>();
    Map<Storage, Set<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage.addToPartCols(Storage.getDatePartition());
    storageAggregatePeriods.put(hdfsStorage, updates);
    updatePeriods.put(hdfsStorage.getName(), updates);

    CubeFactTable cubeFact = new CubeFactTable(cubeName, factName, factColumns,
        updatePeriods);

    // create cube fact
    client.createCubeFactTable(cubeName, factName, factColumns,
        storageAggregatePeriods, 0L, null);
    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeName));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFact.equals(cubeFact2));

    // Assert for storage tables
    for (Storage entry : storageAggregatePeriods.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(
          factName, entry.getPrefix());
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(Storage.getDatePartitionKey(), now);
    // test partition
    client.addPartition(cubeFact, hdfsStorage, UpdatePeriod.HOURLY, timeParts);
    Assert.assertTrue(client.factPartitionExists(cubeFact, hdfsStorage,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
  }

  @Test
  public void testCubeFactWithTwoTimedParts() throws Exception {
    String factName = "testMetastoreFactTimedParts";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode","int", "zip"));

    Map<String, Set<UpdatePeriod>> updatePeriods =
        new HashMap<String, Set<UpdatePeriod>>();
    Map<Storage, Set<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    FieldSchema testDtPart = new FieldSchema("mydate", "string", "date part");
    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage.addToPartCols(Storage.getDatePartition());
    hdfsStorage.addToPartCols(testDtPart);
    storageAggregatePeriods.put(hdfsStorage, updates);
    updatePeriods.put(hdfsStorage.getName(), updates);

    CubeFactTable cubeFact = new CubeFactTable(cubeNameWithProps, factName,
        factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(cubeNameWithProps, factName, factColumns,
        storageAggregatePeriods, 0L, null);
    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeNameWithProps));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFact.equals(cubeFact2));

    // Assert for storage tables
    for (Storage entry : storageAggregatePeriods.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(
          factName, entry.getPrefix());
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    Calendar cal = new GregorianCalendar();
    cal.setTime(now);
    cal.add(Calendar.HOUR, -1);
    Date testDt = cal.getTime();
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(Storage.getDatePartitionKey(), now);
    timeParts.put(testDtPart.getName(), testDt);
    // test partition
    client.addPartition(cubeFact, hdfsStorage, UpdatePeriod.HOURLY, timeParts);
    Assert.assertTrue(client.factPartitionExists(cubeFact, hdfsStorage,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
  }

  @Test
  public void testCubeFactWithWeight() throws Exception {
    String factName = "testFactWithWeight";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode","int", "zip"));

    Map<String, Set<UpdatePeriod>> updatePeriods =
        new HashMap<String, Set<UpdatePeriod>>();
    Map<Storage, Set<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage.addToPartCols(Storage.getDatePartition());
    storageAggregatePeriods.put(hdfsStorage, updates);
    updatePeriods.put(hdfsStorage.getName(), updates);

    CubeFactTable cubeFact = new CubeFactTable(cubeName, factName, factColumns,
        updatePeriods, 100L);

    // create cube fact
    client.createCubeFactTable(cubeName, factName, factColumns,
        storageAggregatePeriods, 100L, null);
    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeName));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFact.equals(cubeFact2));

    // Assert for storage tables
    for (Storage entry : storageAggregatePeriods.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(
          factName, entry.getPrefix());
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    // test partition
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(Storage.getDatePartitionKey(), now);
    client.addPartition(cubeFact, hdfsStorage, UpdatePeriod.HOURLY, timeParts);
    Assert.assertTrue(client.factPartitionExists(cubeFact, hdfsStorage,
        UpdatePeriod.HOURLY, now));
  }


  @Test
  public void testCubeFactWithParts() throws Exception {
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    String factNameWithPart = "testFactPart";

    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add some dimensions of the cube
    factColumns.add(new FieldSchema("dim1","string", "dim1"));
    factColumns.add(new FieldSchema("dim2","string", "dim2"));

    List<FieldSchema> factPartColumns = new ArrayList<FieldSchema>();
    factPartColumns.add(new FieldSchema("region","string", "region part"));

    Map<String, Set<UpdatePeriod>> updatePeriods =
        new HashMap<String, Set<UpdatePeriod>>();
    Map<Storage, Set<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    Storage hdfsStorageWithParts = new HDFSStorage("C1",
        SequenceFileInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorageWithParts.addToPartCols(Storage.getDatePartition());
    hdfsStorageWithParts.addToPartCols(factPartColumns.get(0));
    storageAggregatePeriods.put(hdfsStorageWithParts, updates);
    updatePeriods.put(hdfsStorageWithParts.getName(), updates);

    CubeFactTable cubeFactWithParts = new CubeFactTable(cubeName,
        factNameWithPart, factColumns, updatePeriods);
    client.createCubeFactTable(cubeName, factNameWithPart, factColumns,
        storageAggregatePeriods, 0L, null);
    Assert.assertTrue(client.tableExists(factNameWithPart));
    Table cubeTbl = client.getHiveTable(factNameWithPart);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeName));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFactWithParts.equals(cubeFact2));

    // Assert for storage tables
    for (Storage entry : storageAggregatePeriods.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(
          factNameWithPart, entry.getPrefix());
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put(factPartColumns.get(0).getName(), "APAC");
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(Storage.getDatePartitionKey(), now);
    // test partition
    client.addPartition(cubeFactWithParts, hdfsStorageWithParts,
        UpdatePeriod.HOURLY, timeParts, partSpec);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithParts,
        hdfsStorageWithParts,
        UpdatePeriod.HOURLY, timeParts, partSpec));
  }

  @Test
  public void testCubeFactWithPartsAndTimedParts() throws Exception {
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    String factNameWithPart = "testFactPartAndTimedParts";

    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add some dimensions of the cube
    factColumns.add(new FieldSchema("dim1","string", "dim1"));
    factColumns.add(new FieldSchema("dim2","string", "dim2"));

    List<FieldSchema> factPartColumns = new ArrayList<FieldSchema>();
    factPartColumns.add(new FieldSchema("region","string", "region part"));

    Map<String, Set<UpdatePeriod>> updatePeriods =
        new HashMap<String, Set<UpdatePeriod>>();
    Map<Storage, Set<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    FieldSchema testDtPart = new FieldSchema("mydate", "string", "date part");
    Storage hdfsStorageWithParts = new HDFSStorage("C1",
        SequenceFileInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorageWithParts.addToPartCols(Storage.getDatePartition());
    hdfsStorageWithParts.addToPartCols(testDtPart);
    hdfsStorageWithParts.addToPartCols(factPartColumns.get(0));
    storageAggregatePeriods.put(hdfsStorageWithParts, updates);
    updatePeriods.put(hdfsStorageWithParts.getName(), updates);

    CubeFactTable cubeFactWithParts = new CubeFactTable(cubeName,
        factNameWithPart, factColumns, updatePeriods);
    client.createCubeFactTable(cubeName, factNameWithPart, factColumns,
        storageAggregatePeriods, 0L, null);
    Assert.assertTrue(client.tableExists(factNameWithPart));
    Table cubeTbl = client.getHiveTable(factNameWithPart);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeName));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFactWithParts.equals(cubeFact2));

    // Assert for storage tables
    for (Storage entry :storageAggregatePeriods.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(
          factNameWithPart, entry.getPrefix());
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    Calendar cal = new GregorianCalendar();
    cal.setTime(now);
    cal.add(Calendar.HOUR, -1);
    Date testDt = cal.getTime();
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put(factPartColumns.get(0).getName(), "APAC");
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(Storage.getDatePartitionKey(), now);
    timeParts.put(testDtPart.getName(), testDt);
    // test partition
    client.addPartition(cubeFactWithParts, hdfsStorageWithParts,
        UpdatePeriod.HOURLY, timeParts, partSpec);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithParts,
        hdfsStorageWithParts,
        UpdatePeriod.HOURLY, timeParts, partSpec));
  }

  @Test
  public void testCubeFactWithTwoStorages() throws Exception {
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    String factName = "testFactTwoStorages";

    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add some dimensions of the cube
    factColumns.add(new FieldSchema("dim1","string", "dim1"));
    factColumns.add(new FieldSchema("dim2","string", "dim2"));

    List<FieldSchema> factPartColumns = new ArrayList<FieldSchema>();
    factPartColumns.add(new FieldSchema("region","string", "region part"));

    Map<String, Set<UpdatePeriod>> updatePeriods =
        new HashMap<String, Set<UpdatePeriod>>();
    Map<Storage, Set<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    Storage hdfsStorageWithParts = new HDFSStorage("C1",
        SequenceFileInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorageWithParts.addToPartCols(Storage.getDatePartition());
    hdfsStorageWithParts.addToPartCols(factPartColumns.get(0));
    Storage hdfsStorageWithNoParts = new HDFSStorage("C2",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorageWithNoParts.addToPartCols(Storage.getDatePartition());

    storageAggregatePeriods.put(hdfsStorageWithParts, updates);
    storageAggregatePeriods.put(hdfsStorageWithNoParts, updates);
    updatePeriods.put(hdfsStorageWithParts.getName(), updates);
    updatePeriods.put(hdfsStorageWithNoParts.getName(), updates);

    CubeFactTable cubeFactWithTwoStorages = new CubeFactTable(cubeName,
        factName, factColumns, updatePeriods);
    client.createCubeFactTable(cubeName, factName, factColumns,
        storageAggregatePeriods, 0L, null);
    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeName));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFactWithTwoStorages.equals(cubeFact2));

    // Assert for storage tables
    for (Storage entry : storageAggregatePeriods.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(
          factName, entry.getPrefix());
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put(factPartColumns.get(0).getName(), "APAC");
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(Storage.getDatePartitionKey(), now);
    // test partition
    client.addPartition(cubeFactWithTwoStorages, hdfsStorageWithParts,
        UpdatePeriod.HOURLY, timeParts, partSpec);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithTwoStorages,
        hdfsStorageWithParts,
        UpdatePeriod.HOURLY, timeParts, partSpec));

    client.addPartition(cubeFactWithTwoStorages, hdfsStorageWithNoParts,
        UpdatePeriod.HOURLY, timeParts);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithTwoStorages,
        hdfsStorageWithNoParts,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
  }

  @Test
  public void testCubeDimWithWeight() throws Exception {
    String dimName = "statetable";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "state id"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("capital", "string", "field2"));
    dimColumns.add(new FieldSchema("countryid", "int", "country id"));

    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();
    dimensionReferences.put("countryid", Arrays.asList(new TableReference("countrytable", "id")));

    Map<Storage, UpdatePeriod> snapshotDumpPeriods =
        new HashMap<Storage, UpdatePeriod>();
    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    snapshotDumpPeriods.put(hdfsStorage, UpdatePeriod.HOURLY);
    dumpPeriods.put(hdfsStorage.getName(), UpdatePeriod.HOURLY);
    CubeDimensionTable cubeDim = new CubeDimensionTable(dimName,
        dimColumns, 100L, dumpPeriods, dimensionReferences);
    client.createCubeDimensionTable(dimName, dimColumns, 100L,
        dimensionReferences, snapshotDumpPeriods, null);
    Assert.assertTrue(client.tableExists(dimName));
    Table cubeTbl = client.getHiveTable(dimName);
    Assert.assertTrue(client.isDimensionTable(cubeTbl));
    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    Assert.assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    for (Storage storage : snapshotDumpPeriods.keySet()) {
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimName,
          storage.getPrefix());
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    // test partition
    client.addPartition(cubeDim, hdfsStorage, now);
    Assert.assertTrue(client.dimPartitionExists(cubeDim, hdfsStorage, now));
    Assert.assertTrue(client.latestPartitionExists(cubeDim, hdfsStorage));
  }

  @Test
  public void testCubeDim() throws Exception {
    String dimName = "ziptableMeta";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("zipcode", "int", "code"));
    dimColumns.add(new FieldSchema("f1", "string", "field1"));
    dimColumns.add(new FieldSchema("f2", "string", "field2"));
    dimColumns.add(new FieldSchema("stateid", "int", "state id"));
    dimColumns.add(new FieldSchema("statei2", "int", "state id"));

    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();


    dimensionReferences.put("stateid", Arrays.asList(new TableReference("statetable", "id")));

    final TableReference stateRef = new TableReference("statetable", "id");
    final TableReference cityRef =  new TableReference("citytable", "id");
    dimensionReferences.put("stateid2",
        Arrays.asList(stateRef, cityRef));

    Map<Storage, UpdatePeriod> snapshotDumpPeriods =
        new HashMap<Storage, UpdatePeriod>();
    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    snapshotDumpPeriods.put(hdfsStorage, UpdatePeriod.HOURLY);

    dumpPeriods.put(hdfsStorage.getName(), UpdatePeriod.HOURLY);

    CubeDimensionTable cubeDim = new CubeDimensionTable(dimName,
        dimColumns, 0L, dumpPeriods, dimensionReferences);

    client.createCubeDimensionTable(dimName, dimColumns, 0L,
        dimensionReferences, snapshotDumpPeriods, null);

    Assert.assertTrue(client.tableExists(dimName));

    Table cubeTbl = client.getHiveTable(dimName);
    Assert.assertTrue(client.isDimensionTable(cubeTbl));

    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    Assert.assertTrue(cubeDim.equals(cubeDim2));

    Map<String, List<TableReference>> actualRefs = cubeDim2.getDimensionReferences();
    Assert.assertNotNull(actualRefs);
    List<TableReference> multirefs = actualRefs.get("stateid2");
    Assert.assertNotNull(multirefs);
    Assert.assertEquals("Expecting two refs", 2, multirefs.size());
    Assert.assertEquals(stateRef, multirefs.get(0));
    Assert.assertEquals(cityRef, multirefs.get(1));


    // Assert for storage tables
    for (Storage storage : snapshotDumpPeriods.keySet()) {
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimName,
          storage.getPrefix());
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    // test partition
    client.addPartition(cubeDim, hdfsStorage, now);
    Assert.assertTrue(client.dimPartitionExists(cubeDim, hdfsStorage, now));
    Assert.assertTrue(client.latestPartitionExists(cubeDim, hdfsStorage));
  }


  @Test
  public void testCubeDimWithoutDumps() throws Exception {
    String dimName = "countrytableMeta";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("capital", "string", "field2"));
    dimColumns.add(new FieldSchema("region", "string", "region name"));

    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();
    dimensionReferences.put("stateid", Arrays.asList(new TableReference("statetable", "id")));

    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Set<Storage> storages = new HashSet<Storage>();
    Set<String> storageNames = new HashSet<String>();
    storages.add(hdfsStorage);
    storageNames.add(hdfsStorage.getName());
    CubeDimensionTable cubeDim = new CubeDimensionTable(dimName,
        dimColumns, 0L, storageNames, dimensionReferences);
    client.createCubeDimensionTable(dimName, dimColumns, 0L,
        dimensionReferences, storages, null);
    Assert.assertTrue(client.tableExists(dimName));
    Table cubeTbl = client.getHiveTable(dimName);
    Assert.assertTrue(client.isDimensionTable(cubeTbl));
    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    Assert.assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    for (Storage storage : storages) {
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimName,
          storage.getPrefix());
      Assert.assertTrue(client.tableExists(storageTableName));
      Assert.assertTrue(!client.getHiveTable(storageTableName).isPartitioned());
    }
  }

  @Test
  public void testCubeDimWithTwoStorages() throws Exception {
    String dimName = "citytableMeta";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("stateid", "int", "state id"));

    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();
    dimensionReferences.put("stateid", Arrays.asList(new TableReference("statetable", "id")));

    Storage hdfsStorage1 = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Storage hdfsStorage2 = new HDFSStorage("C2",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Map<Storage, UpdatePeriod> snapshotDumpPeriods =
        new HashMap<Storage, UpdatePeriod>();
    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    snapshotDumpPeriods.put(hdfsStorage1, UpdatePeriod.HOURLY);
    dumpPeriods.put(hdfsStorage1.getName(), UpdatePeriod.HOURLY);
    snapshotDumpPeriods.put(hdfsStorage2, null);
    dumpPeriods.put(hdfsStorage2.getName(), null);
    CubeDimensionTable cubeDim = new CubeDimensionTable(dimName,
        dimColumns, 0L, dumpPeriods, dimensionReferences);
    client.createCubeDimensionTable(dimName, dimColumns, 0L,
        dimensionReferences, snapshotDumpPeriods, null);
    Assert.assertTrue(client.tableExists(dimName));
    Table cubeTbl = client.getHiveTable(dimName);
    Assert.assertTrue(client.isDimensionTable(cubeTbl));
    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    Assert.assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    String storageTableName1 = MetastoreUtil.getDimStorageTableName(dimName,
        hdfsStorage1.getPrefix());
    Assert.assertTrue(client.tableExists(storageTableName1));
    String storageTableName2 = MetastoreUtil.getDimStorageTableName(dimName,
        hdfsStorage2.getPrefix());
    Assert.assertTrue(client.tableExists(storageTableName2));
    Assert.assertTrue(!client.getHiveTable(storageTableName2).isPartitioned());
  }

}
