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
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("deprecation")
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
  private static Date nowPlus1;
  private static HiveConf conf = new HiveConf(TestCubeMetastoreClient.class);

  @BeforeClass
  public static void setup() throws HiveException, AlreadyExistsException {
    SessionState.start(conf);
    client =  CubeMetastoreClient.getInstance(conf);
    now = new Date();
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.HOUR_OF_DAY, 1);
    nowPlus1 = cal.getTime();
    Database database = new Database();
    database.setName(TestCubeMetastoreClient.class.getSimpleName());
    Hive.get(conf).createDatabase(database);
    client.setCurrentDatabase(TestCubeMetastoreClient.class.getSimpleName());
    defineCube(cubeName, cubeNameWithProps);
  }

  @AfterClass
  public static void teardown() throws Exception {
    Hive.get().dropDatabase(TestCubeMetastoreClient.class.getSimpleName(),
        true, true, true);
    CubeMetastoreClient.close();
  }

  private static void defineCube(String cubeName, String cubeNameWithProps) {
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
  public void testAlterCube() throws Exception {
    String cubeName = "alter_test_cube";
    client.createCube(cubeName, cubeMeasures, cubeDimensions);
    // Test alter cube
    Table cubeTbl = client.getHiveTable(cubeName);
    Cube toAlter = new Cube(cubeTbl);
    toAlter.alterMeasure(new ColumnMeasure(new FieldSchema("testAddMsr1",
        "int", "testAddMeasure")));
    toAlter.alterMeasure(new ColumnMeasure(new FieldSchema("msr3", "float",
        "third altered measure"),
        null, "MAX", "alterunit"));
    toAlter.removeMeasure("msr4");
    toAlter.alterDimension(new BaseDimension(new FieldSchema("testAddDim1",
        "string", "dim to add")));
    toAlter.alterDimension(new BaseDimension(new FieldSchema("dim1", "int",
        "basedim altered")));
    toAlter.removeDimension("location2");
    toAlter.addTimedDimension("zt");
    toAlter.removeTimedDimension("dt");

    Assert.assertNotNull(toAlter.getMeasureByName("testAddMsr1"));
    Assert.assertNotNull(toAlter.getMeasureByName("msr3"));
    Assert.assertNull(toAlter.getMeasureByName("msr4"));
    Assert.assertNotNull(toAlter.getDimensionByName("testAddDim1"));
    Assert.assertNotNull(toAlter.getDimensionByName("dim1"));
    Assert.assertNull(toAlter.getDimensionByName("location2"));

    client.alterCube(cubeName, toAlter);

    Table alteredHiveTbl = Hive.get(conf).getTable(cubeName);

    Cube altered = new Cube(alteredHiveTbl);

    Assert.assertEquals(toAlter, altered);
    Assert.assertNotNull(altered.getMeasureByName("testAddMsr1"));
    CubeMeasure addedMsr = altered.getMeasureByName("testAddMsr1");
    Assert.assertEquals(addedMsr.getType(), "int");
    Assert.assertNotNull(altered.getDimensionByName("testAddDim1"));
    BaseDimension addedDim = (BaseDimension) altered.getDimensionByName("testAddDim1");
    Assert.assertEquals(addedDim.getType(), "string");
    Assert.assertTrue(altered.getTimedDimensions().contains("zt"));

    toAlter.alterMeasure(new ColumnMeasure(new FieldSchema("testAddMsr1", "double", "testAddMeasure")));
    client.alterCube(cubeName, toAlter);
    altered = new Cube(Hive.get(conf).getTable(cubeName));
    addedMsr = altered.getMeasureByName("testaddmsr1");
    Assert.assertNotNull(addedMsr);
    Assert.assertEquals(addedMsr.getType(), "double");
    // Drop the table
    client.dropCube(cubeName, true);
    client = CubeMetastoreClient.getInstance(conf);
    Assert.assertFalse(client.tableExists(cubeName));
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
    client.addPartition(cubeFact, hdfsStorage, UpdatePeriod.HOURLY, timeParts,
        Storage.getDatePartitionKey());
    Assert.assertTrue(client.factPartitionExists(cubeFact, hdfsStorage,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact, hdfsStorage,
        Storage.getDatePartitionKey()));

    // Partition with different schema
    FieldSchema newcol = new FieldSchema("newcol", "int", "new col for part");
    cubeFact.alterColumn(newcol);
    client.alterCubeFactTable(cubeFact.getName(), cubeFact);
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        factName, hdfsStorage.getPrefix());
    List<Partition> parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertFalse(parts.get(0).getCols().contains(newcol));

    Storage hdfsStorage2 = new HDFSStorage("C1",
        SequenceFileInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage2.addToPartCols(Storage.getDatePartition());
    Map<String, Date> timeParts2 = new HashMap<String, Date>();
    timeParts2.put(Storage.getDatePartitionKey(), nowPlus1);
    client.addPartition(cubeFact, hdfsStorage2, UpdatePeriod.HOURLY, timeParts2,
        Storage.getDatePartitionKey());
    Assert.assertTrue(client.factPartitionExists(cubeFact, hdfsStorage,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
    Assert.assertTrue(client.factPartitionExists(cubeFact, hdfsStorage2,
        UpdatePeriod.HOURLY, timeParts2, new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact, hdfsStorage2,
        Storage.getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(SequenceFileInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertTrue(parts.get(0).getCols().contains(newcol));
  }

  @Test
  public void testAlterCubeFact() throws Exception {
    String factName = "test_alter_fact";
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
    Storage hdfsStorage2 = new HDFSStorage("C2",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage2.addToPartCols(Storage.getDatePartition());
    storageAggregatePeriods.put(hdfsStorage2, updates);
    updatePeriods.put(hdfsStorage2.getName(), updates);

    // create cube fact
    client.createCubeFactTable(cubeName, factName, factColumns,
      storageAggregatePeriods, 0L, null);

    CubeFactTable factTable = new CubeFactTable(Hive.get(conf).getTable(factName));
    factTable.alterColumn(new FieldSchema("testFactColAdd", "int", "test add column"));
    factTable.alterColumn(new FieldSchema("msr3", "int", "test alter column"));
    factTable.alterWeight(100L);
    Map<String, String> newProp = new HashMap<String, String>();
    newProp.put("new.prop", "val");
    factTable.addProperties(newProp);
    factTable.addUpdatePeriod(hdfsStorage.getName(), UpdatePeriod.MONTHLY);
    factTable.removeUpdatePeriod(hdfsStorage.getName(), UpdatePeriod.HOURLY);
    Set<UpdatePeriod> alterupdates  = new HashSet<UpdatePeriod>();
    alterupdates.add(UpdatePeriod.HOURLY);
    alterupdates.add(UpdatePeriod.DAILY);
    alterupdates.add(UpdatePeriod.MONTHLY);
    factTable.alterStorage(hdfsStorage2.getName(), alterupdates);

    client.alterCubeFactTable(factName, factTable);

    Table factHiveTable = Hive.get(conf).getTable(factName);
    CubeFactTable altered = new CubeFactTable(factHiveTable);

    Assert.assertTrue(altered.weight() == 100L);
    Assert.assertTrue(altered.getProperties().get("new.prop").equals("val"));
    Assert.assertTrue(altered.getUpdatePeriods().get(hdfsStorage.getName())
        .contains(UpdatePeriod.MONTHLY));
    Assert.assertFalse(altered.getUpdatePeriods().get(hdfsStorage.getName())
        .contains(UpdatePeriod.HOURLY));
    Assert.assertTrue(altered.getUpdatePeriods().get(hdfsStorage2.getName())
        .contains(UpdatePeriod.MONTHLY));
    Assert.assertTrue(altered.getUpdatePeriods().get(hdfsStorage2.getName())
        .contains(UpdatePeriod.DAILY));
    Assert.assertTrue(altered.getUpdatePeriods().get(hdfsStorage2.getName())
        .contains(UpdatePeriod.HOURLY));
    boolean contains = false;
    for (FieldSchema column : altered.getColumns()) {
      if (column.getName().equals("testfactcoladd") && column.getType().equals("int")) {
        contains = true;
        break;
      }
    }
    Assert.assertTrue(contains);

    Storage hdfsStorage3 = new HDFSStorage("C3",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage3.addToPartCols(Storage.getDatePartition());
    client.addStorage(altered, hdfsStorage3, updates);
    Assert.assertTrue(altered.getStorages().contains("C3"));
    Assert.assertTrue(altered.getUpdatePeriods().get("C3").equals(updates));
    String storageTableName = MetastoreUtil.getFactStorageTableName(
          factName, hdfsStorage3.getPrefix());
    Assert.assertTrue(client.tableExists(storageTableName));
    client.dropStorageFromFact(factName, "C2");
    storageTableName = MetastoreUtil.getFactStorageTableName(
        factName, hdfsStorage2.getPrefix());
    Assert.assertFalse(client.tableExists(storageTableName));
    client.dropFact(factName, true);
    Assert.assertFalse(client.tableExists(MetastoreUtil.getFactStorageTableName(
        factName, hdfsStorage.getPrefix())));
    Assert.assertFalse(client.tableExists(MetastoreUtil.getFactStorageTableName(
        factName, hdfsStorage3.getPrefix())));
    Assert.assertFalse(client.tableExists(factName));
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
    client.addPartition(cubeFact, hdfsStorage, UpdatePeriod.HOURLY, timeParts,
        testDtPart.getName());
    Assert.assertTrue(client.factPartitionExists(cubeFact, hdfsStorage,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact, hdfsStorage,
        testDtPart.getName()));
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
    client.addPartition(cubeFact, hdfsStorage, UpdatePeriod.HOURLY, timeParts,
        null);
    Assert.assertTrue(client.factPartitionExists(cubeFact, hdfsStorage,
        UpdatePeriod.HOURLY, now));
    Assert.assertFalse(client.latestPartitionExists(cubeFact, hdfsStorage,
        Storage.getDatePartitionKey()));
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
        UpdatePeriod.HOURLY, timeParts, partSpec, null);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithParts,
        hdfsStorageWithParts,
        UpdatePeriod.HOURLY, timeParts, partSpec));
    Assert.assertFalse(client.latestPartitionExists(cubeFactWithParts,
        hdfsStorageWithParts,
        Storage.getDatePartitionKey()));
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
        UpdatePeriod.HOURLY, timeParts, partSpec, null);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithParts,
        hdfsStorageWithParts,
        UpdatePeriod.HOURLY, timeParts, partSpec));
    Assert.assertFalse(client.latestPartitionExists(cubeFactWithParts,
        hdfsStorageWithParts,
        Storage.getDatePartitionKey()));
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
      UpdatePeriod.HOURLY, timeParts, partSpec, Storage.getDatePartitionKey());
    Assert.assertTrue(client.factPartitionExists(cubeFactWithTwoStorages,
      hdfsStorageWithParts,
      UpdatePeriod.HOURLY, timeParts, partSpec));
    Assert.assertTrue(client.latestPartitionExists(cubeFactWithTwoStorages,
        hdfsStorageWithParts,
        Storage.getDatePartitionKey()));

    client.addPartition(cubeFactWithTwoStorages, hdfsStorageWithNoParts,
        UpdatePeriod.HOURLY, timeParts, Storage.getDatePartitionKey());
    Assert.assertTrue(client.factPartitionExists(cubeFactWithTwoStorages,
        hdfsStorageWithNoParts,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFactWithTwoStorages,
      hdfsStorageWithParts,
      Storage.getDatePartitionKey()));
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

    // Partition with different schema
    FieldSchema newcol = new FieldSchema("newcol", "int", "new col for part");
    String storageTableName = MetastoreUtil.getDimStorageTableName(
        dimName, hdfsStorage.getPrefix());
    List<Partition> parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertFalse(parts.get(0).getCols().contains(newcol));
    cubeDim.alterColumn(newcol);
    client.alterCubeDimensionTable(cubeDim.getName(), cubeDim);
    Storage hdfsStorage2 = new HDFSStorage("C1",
        SequenceFileInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage2.addToPartCols(Storage.getDatePartition());
    client.addPartition(cubeDim, hdfsStorage2, nowPlus1);
    Assert.assertTrue(client.dimPartitionExists(cubeDim, hdfsStorage, now));
    Assert.assertTrue(client.dimPartitionExists(cubeDim, hdfsStorage2, nowPlus1));
    Assert.assertTrue(client.latestPartitionExists(cubeDim, hdfsStorage));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(SequenceFileInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertTrue(parts.get(0).getCols().contains(newcol));
  }

  @Test
  public void testAlterDim() throws Exception {
    String dimName = "test_alter_dim";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("zipcode", "int", "code"));

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
    CubeDimensionTable dimTable = client.getDimensionTable(dimName);
    dimTable.alterColumn(new FieldSchema("testAddDim", "string", "test add column"));

    client.alterCubeDimensionTable(dimName, dimTable);

    Table alteredHiveTable = Hive.get(conf).getTable(dimName);
    CubeDimensionTable altered = new CubeDimensionTable(alteredHiveTable);
    List<FieldSchema> columns = altered.getColumns();
    boolean contains = false;
    for (FieldSchema column : columns) {
      if (column.getName().equals("testadddim") && column.getType().equals("string")) {
        contains = true;
        break;
      }
    }
    Assert.assertTrue(contains);

    // Test alter column
    dimTable.alterColumn(new FieldSchema("testAddDim", "int", "change type"));
    client.alterCubeDimensionTable(dimName, dimTable);

    altered = new CubeDimensionTable(Hive.get(conf).getTable(dimName));
    boolean typeChanged = false;
    for (FieldSchema column : altered.getColumns()) {
      if (column.getName().equals("testadddim") && column.getType().equals("int")) {
        typeChanged = true;
        break;
      }
    }
    Assert.assertTrue(typeChanged);
    Storage hdfsStorage2 = new HDFSStorage("C2",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    client.addStorage(dimTable, hdfsStorage2, null);
    Storage hdfsStorage3 = new HDFSStorage("C3",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    client.addStorage(dimTable, hdfsStorage3, UpdatePeriod.DAILY);
    Assert.assertTrue(client.tableExists(MetastoreUtil.getDimStorageTableName(
        dimName, hdfsStorage2.getPrefix())));
    Assert.assertTrue(client.tableExists(MetastoreUtil.getDimStorageTableName(
        dimName, hdfsStorage3.getPrefix())));
    Assert.assertFalse(dimTable.hasStorageSnapshots("C2"));
    Assert.assertTrue(dimTable.hasStorageSnapshots("C3"));
    client.dropStorageFromDim(dimName, "C1");
    Assert.assertFalse(client.tableExists(MetastoreUtil.getDimStorageTableName(
        dimName, hdfsStorage.getPrefix())));
    client.dropDimension(dimName, true);
    Assert.assertFalse(client.tableExists(MetastoreUtil.getDimStorageTableName(
        dimName, hdfsStorage2.getPrefix())));
    Assert.assertFalse(client.tableExists(MetastoreUtil.getDimStorageTableName(
        dimName, hdfsStorage3.getPrefix())));
    Assert.assertFalse(client.tableExists(dimName));
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

  @Test
  public void testCaching() throws HiveException {
    client = CubeMetastoreClient.getInstance(conf);
    List<Cube> cubes = client.getAllCubes();
    Assert.assertEquals(2, cubes.size());
    defineCube("testcache1", "testcache2");
    client.createCube("testcache1", cubeMeasures, cubeDimensions);
    client.createCube("testcache2", cubeMeasures, cubeDimensions, cubeProperties);
    cubes = client.getAllCubes();
    Assert.assertEquals(2, cubes.size());
    client = CubeMetastoreClient.getInstance(conf);
    cubes = client.getAllCubes();
    Assert.assertEquals(2, cubes.size());
    conf.setBoolean(MetastoreConstants.METASTORE_NEEDS_REFRESH, true);
    client = CubeMetastoreClient.getInstance(conf);
    cubes = client.getAllCubes();
    Assert.assertEquals(4, cubes.size());
    conf.setBoolean(MetastoreConstants.METASTORE_ENABLE_CACHING, false);
    client = CubeMetastoreClient.getInstance(conf);
    cubes = client.getAllCubes();
    Assert.assertEquals(4, cubes.size());
    defineCube("testcache3", "testcache4");
    client.createCube("testcache3", cubeMeasures, cubeDimensions);
    client.createCube("testcache4", cubeMeasures, cubeDimensions, cubeProperties);
    cubes = client.getAllCubes();
    Assert.assertEquals(6, cubes.size());
  }
}
