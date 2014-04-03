package org.apache.hadoop.hive.ql.cube.metadata;
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
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
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
  private static FieldSchema dtPart = new FieldSchema(getDatePartitionKey(),
      serdeConstants.STRING_TYPE_NAME,
      "date partition");
  private static String c1 = "C1";
  private static String c2 = "C2";
  private static String c3 = "C3";

  /**
   * Get the date partition as fieldschema
   *
   * @return FieldSchema
   */
  public static FieldSchema getDatePartition() {
    return TestCubeMetastoreClient.dtPart;
  }

  /**
   * Get the date partition key
   *
   * @return String
   */
  public static String getDatePartitionKey() {
    return StorageConstants.DATE_PARTITION_KEY;
  }

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
    // Drop the cube
    client.dropCube(cubeName);
    client = CubeMetastoreClient.getInstance(conf);
    Assert.assertFalse(client.tableExists(cubeName));

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
  public void testStorage() throws Exception {
    Storage hdfsStorage =  new HDFSStorage(c1);
    client.createStorage(hdfsStorage);
    Assert.assertEquals(1, client.getAllStorages().size());

    Storage hdfsStorage2 =  new HDFSStorage(c2);
    client.createStorage(hdfsStorage2);
    Assert.assertEquals(2, client.getAllStorages().size());

    Storage hdfsStorage3 =  new HDFSStorage(c3);
    client.createStorage(hdfsStorage3);
    Assert.assertEquals(3, client.getAllStorages().size());

    Assert.assertEquals(hdfsStorage, client.getStorage(c1));
    Assert.assertEquals(hdfsStorage2, client.getStorage(c2));
    Assert.assertEquals(hdfsStorage3, client.getStorage(c3));
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
    Assert.assertTrue(client.getAllFactTables(altered).isEmpty());
  }

  @Test
  public void testCubeFact() throws Exception {
    String factName = "testMetastoreFact";
    List<String> cubeNames = new ArrayList<String>();
    cubeNames.add(cubeName);
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode","int", "zip"));

    Map<String, Set<UpdatePeriod>> updatePeriods =
        new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    timePartCols.add(getDatePartitionKey());
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);
    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    CubeFactTable cubeFact = new CubeFactTable(cubeNames, factName, factColumns,
        updatePeriods);

    // create cube fact
    client.createCubeFactTable(cubeNames, factName, factColumns,
        updatePeriods, 0L, null, storageTables);
    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeName));
    Assert.assertEquals(client.getAllFactTables(
        client.getCube(cubeName)).get(0).getName(), factName.toLowerCase());
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFact.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(
          factName, entry);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    // test partition
    StoragePartitionDesc partSpec = new StoragePartitionDesc(cubeFact.getName(),
        timeParts, null, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertTrue(client.factPartitionExists(cubeFact.getName(), c1,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));

    // Partition with different schema
    FieldSchema newcol = new FieldSchema("newcol", "int", "new col for part");
    cubeFact.alterColumn(newcol);
    client.alterCubeFactTable(cubeFact.getName(), cubeFact);
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        factName, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertFalse(parts.get(0).getCols().contains(newcol));
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));

    Map<String, Date> timeParts2 = new HashMap<String, Date>();
    timeParts2.put(TestCubeMetastoreClient.getDatePartitionKey(), nowPlus1);
    StoragePartitionDesc partSpec2 = new StoragePartitionDesc(cubeFact.getName(),
        timeParts2, null, UpdatePeriod.HOURLY);
    partSpec2.setInputFormat(SequenceFileInputFormat.class.getCanonicalName());
    partSpec2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    client.addPartition(partSpec2, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 3);
    Assert.assertTrue(client.factPartitionExists(cubeFact.getName(), c1,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
    Assert.assertTrue(client.factPartitionExists(cubeFact.getName(), c1,
        UpdatePeriod.HOURLY, timeParts2, new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(SequenceFileInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertTrue(parts.get(0).getCols().contains(newcol));
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(nowPlus1));

    client.dropPartition(cubeFact.getName(), c1, timeParts2, null,
        UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);
    Assert.assertTrue(client.factPartitionExists(cubeFact.getName(), c1,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
    Assert.assertFalse(client.factPartitionExists(cubeFact.getName(), c1,
        UpdatePeriod.HOURLY, timeParts2, new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertFalse(parts.get(0).getCols().contains(newcol));
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));

    client.dropPartition(cubeFact.getName(), c1, timeParts, null,
        UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
    Assert.assertFalse(client.factPartitionExists(cubeFact.getName(), c1,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
    Assert.assertFalse(client.factPartitionExists(cubeFact.getName(), c1,
        UpdatePeriod.HOURLY, timeParts2, new HashMap<String, String>()));
    Assert.assertFalse(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
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
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);

    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    timePartCols.add(getDatePartitionKey());
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);

    updatePeriods.put(c2, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s1);

    List<String> cubeNames = new ArrayList<String>();
    cubeNames.add(cubeName);

    // create cube fact
    client.createCubeFactTable(cubeNames, factName, factColumns,
        updatePeriods, 0L, null, storageTables);

    CubeFactTable factTable = new CubeFactTable(Hive.get(conf).getTable(factName));
    factTable.alterColumn(new FieldSchema("testFactColAdd", "int", "test add column"));
    factTable.alterColumn(new FieldSchema("msr3", "int", "test alter column"));
    factTable.alterWeight(100L);
    factTable.addCubeName(cubeNameWithProps);
    Map<String, String> newProp = new HashMap<String, String>();
    newProp.put("new.prop", "val");
    factTable.addProperties(newProp);
    factTable.addUpdatePeriod(c1, UpdatePeriod.MONTHLY);
    factTable.removeUpdatePeriod(c1, UpdatePeriod.HOURLY);
    Set<UpdatePeriod> alterupdates  = new HashSet<UpdatePeriod>();
    alterupdates.add(UpdatePeriod.HOURLY);
    alterupdates.add(UpdatePeriod.DAILY);
    alterupdates.add(UpdatePeriod.MONTHLY);
    factTable.alterStorage(c2, alterupdates);

    client.alterCubeFactTable(factName, factTable);

    Table factHiveTable = Hive.get(conf).getTable(factName);
    CubeFactTable altered = new CubeFactTable(factHiveTable);

    Assert.assertTrue(altered.weight() == 100L);
    Assert.assertTrue(altered.getProperties().get("new.prop").equals("val"));
    Assert.assertTrue(altered.getUpdatePeriods().get(c1)
        .contains(UpdatePeriod.MONTHLY));
    Assert.assertFalse(altered.getUpdatePeriods().get(c1)
        .contains(UpdatePeriod.HOURLY));
    Assert.assertTrue(altered.getUpdatePeriods().get(c2)
        .contains(UpdatePeriod.MONTHLY));
    Assert.assertTrue(altered.getUpdatePeriods().get(c2)
        .contains(UpdatePeriod.DAILY));
    Assert.assertTrue(altered.getUpdatePeriods().get(c2)
        .contains(UpdatePeriod.HOURLY));
    Assert.assertEquals(altered.getCubeNames().size(), 2);
    Assert.assertTrue(altered.getCubeNames().contains(cubeName.toLowerCase()));
    Assert.assertTrue(altered.getCubeNames().contains(cubeNameWithProps.toLowerCase()));
    boolean contains = false;
    for (FieldSchema column : altered.getColumns()) {
      if (column.getName().equals("testfactcoladd") && column.getType().equals("int")) {
        contains = true;
        break;
      }
    }
    Assert.assertTrue(contains);

    client.addStorage(altered, c3, updates, s1);
    Assert.assertTrue(altered.getStorages().contains("C3"));
    Assert.assertTrue(altered.getUpdatePeriods().get("C3").equals(updates));
    String storageTableName = MetastoreUtil.getFactStorageTableName(
          factName, c3);
    Assert.assertTrue(client.tableExists(storageTableName));
    client.dropStorageFromFact(factName, c2);
    storageTableName = MetastoreUtil.getFactStorageTableName(
        factName, c2);
    Assert.assertFalse(client.tableExists(storageTableName));
    List<CubeFactTable> cubeFacts = client.getAllFactTables(client.getCube(cubeName));
    List<String> cubeFactNames = new ArrayList<String>();
    for (CubeFactTable cfact : cubeFacts) {
      cubeFactNames.add(cfact.getName());
    }
    Assert.assertTrue(cubeFactNames.contains(factName.toLowerCase()));
    client.dropFact(factName, true);
    Assert.assertFalse(client.tableExists(MetastoreUtil.getFactStorageTableName(
        factName, c1)));
    Assert.assertFalse(client.tableExists(MetastoreUtil.getFactStorageTableName(
        factName, c3)));
    Assert.assertFalse(client.tableExists(factName));
    cubeFacts = client.getAllFactTables(cube);
    cubeFactNames = new ArrayList<String>();
    for (CubeFactTable cfact : cubeFacts) {
      cubeFactNames.add(cfact.getName());
    }
    Assert.assertFalse(cubeFactNames.contains(factName.toLowerCase()));
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
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    FieldSchema testDtPart = new FieldSchema("mydate", "string", "date part");
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    partCols.add(testDtPart);
    timePartCols.add(getDatePartitionKey());
    timePartCols.add(testDtPart.getName());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    List<String> cubeNames = new ArrayList<String>();
    cubeNames.add(cubeNameWithProps);

    CubeFactTable cubeFact = new CubeFactTable(cubeNames, factName,
        factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(cubeNames, factName, factColumns,
        updatePeriods, 0L, null, storageTables);

    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeNameWithProps));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFact.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(
          factName, entry);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    // test partition
    Calendar cal = new GregorianCalendar();
    cal.setTime(now);
    cal.add(Calendar.HOUR, -1);
    Date testDt = cal.getTime();
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts.put(testDtPart.getName(), testDt);
    StoragePartitionDesc partSpec = new StoragePartitionDesc(cubeFact.getName(),
        timeParts, null, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertTrue(client.factPartitionExists(cubeFact.getName(), c1,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        testDtPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        cubeFact.getName(), c1);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 3);
    parts = client.getPartitionsByFilter(storageTableName,
        testDtPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey(testDtPart.getName())),
        UpdatePeriod.HOURLY.format().format(testDt));

    client.dropPartition(cubeFact.getName(), c1, timeParts, null,
        UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
    Assert.assertFalse(client.factPartitionExists(cubeFact.getName(), c1,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
    Assert.assertFalse(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertFalse(client.latestPartitionExists(cubeFact.getName(), c1,
        testDtPart.getName()));
  }

  @Test
  public void testCubeFactWithThreeTimedParts() throws Exception {
    String factName = "testMetastoreFact3TimedParts";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode","int", "zip"));

    Map<String, Set<UpdatePeriod>> updatePeriods =
        new HashMap<String, Set<UpdatePeriod>>();
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    FieldSchema itPart = new FieldSchema("it", "string", "date part");
    FieldSchema etPart = new FieldSchema("et", "string", "date part");
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    partCols.add(itPart);
    partCols.add(etPart);
    timePartCols.add(getDatePartitionKey());
    timePartCols.add(itPart.getName());
    timePartCols.add(etPart.getName());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    List<String> cubeNames = new ArrayList<String>();
    cubeNames.add(cubeNameWithProps);

    CubeFactTable cubeFact = new CubeFactTable(cubeNames, factName,
        factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(cubeNames, factName, factColumns,
        updatePeriods, 0L, null, storageTables);

    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeNameWithProps));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFact.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(
          factName, entry);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    String storageTableName = MetastoreUtil.getFactStorageTableName(
        cubeFact.getName(), c1);

    // test partition
    Calendar cal = new GregorianCalendar();
    cal.setTime(now);
    cal.add(Calendar.HOUR, -1);
    Date nowMinus1 = cal.getTime();
    cal.add(Calendar.HOUR, -1);
    Date nowMinus2 = cal.getTime();

    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts.put(itPart.getName(), now);
    timeParts.put(etPart.getName(), now);
    StoragePartitionDesc partSpec = new StoragePartitionDesc(cubeFact.getName(),
        timeParts, null, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 4);

    Map<String, Date> timeParts2 = new HashMap<String, Date>();
    timeParts2.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts2.put(etPart.getName(), nowPlus1);
    Map<String, String> nonTimeSpec = new HashMap<String, String>();
    nonTimeSpec.put(itPart.getName(), "default");
    partSpec = new StoragePartitionDesc(cubeFact.getName(),
        timeParts2, nonTimeSpec, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 5);

    Map<String, Date> timeParts3 = new HashMap<String, Date>();
    timeParts3.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts3.put(etPart.getName(), now);
    partSpec = new StoragePartitionDesc(cubeFact.getName(),
        timeParts3, nonTimeSpec, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 6);

    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        etPart.getName()));
    List<Partition> parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1), "default");
    Assert.assertEquals(parts.get(0).getValues().get(2),
        UpdatePeriod.HOURLY.format().format(now));
    parts = client.getPartitionsByFilter(storageTableName,
        itPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey(itPart.getName())),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(0),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(2),
        UpdatePeriod.HOURLY.format().format(now));
    parts = client.getPartitionsByFilter(storageTableName,
        etPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey(etPart.getName())),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(0),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1),
        "default");

    Map<String, Date> timeParts4 = new HashMap<String, Date>();
    timeParts4.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts4.put(itPart.getName(), nowPlus1);
    timeParts4.put(etPart.getName(), nowMinus1);
    partSpec = new StoragePartitionDesc(cubeFact.getName(),
        timeParts4, null, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 7);

    Map<String, Date> timeParts5 = new HashMap<String, Date>();
    timeParts5.put(TestCubeMetastoreClient.getDatePartitionKey(), nowPlus1);
    timeParts5.put(itPart.getName(), nowMinus1);
    timeParts5.put(etPart.getName(), nowMinus2);
    partSpec = new StoragePartitionDesc(cubeFact.getName(),
        timeParts5, null, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 8);

    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        etPart.getName()));
    parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(1),
        UpdatePeriod.HOURLY.format().format(nowMinus1));
    Assert.assertEquals(parts.get(0).getValues().get(2),
        UpdatePeriod.HOURLY.format().format(nowMinus2));

    parts = client.getPartitionsByFilter(storageTableName,
        itPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey(itPart.getName())),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(0),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(2),
        UpdatePeriod.HOURLY.format().format(nowMinus1));
    parts = client.getPartitionsByFilter(storageTableName,
        etPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey(etPart.getName())),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(0),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1),
        "default");

    client.dropPartition(cubeFact.getName(), c1, timeParts, null,
        UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 7);
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        etPart.getName()));
    parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(1),
        UpdatePeriod.HOURLY.format().format(nowMinus1));
    Assert.assertEquals(parts.get(0).getValues().get(2),
        UpdatePeriod.HOURLY.format().format(nowMinus2));

    parts = client.getPartitionsByFilter(storageTableName,
        itPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey(itPart.getName())),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(0),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(2),
        UpdatePeriod.HOURLY.format().format(nowMinus1));
    parts = client.getPartitionsByFilter(storageTableName,
        etPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey(etPart.getName())),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(0),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1),
        "default");

    client.dropPartition(cubeFact.getName(), c1, timeParts2, nonTimeSpec,
        UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 6);
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        etPart.getName()));
    parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(1),
        UpdatePeriod.HOURLY.format().format(nowMinus1));
    Assert.assertEquals(parts.get(0).getValues().get(2),
        UpdatePeriod.HOURLY.format().format(nowMinus2));

    parts = client.getPartitionsByFilter(storageTableName,
        itPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey(itPart.getName())),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(0),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(2),
        UpdatePeriod.HOURLY.format().format(nowMinus1));

    parts = client.getPartitionsByFilter(storageTableName,
        etPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey(etPart.getName())),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(0),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1),
        "default");

    client.dropPartition(cubeFact.getName(), c1, timeParts4, null,
        UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 5);
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        etPart.getName()));
    parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(1),
        UpdatePeriod.HOURLY.format().format(nowMinus1));
    Assert.assertEquals(parts.get(0).getValues().get(2),
        UpdatePeriod.HOURLY.format().format(nowMinus2));

    parts = client.getPartitionsByFilter(storageTableName,
        itPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey(itPart.getName())),
        UpdatePeriod.HOURLY.format().format(nowMinus1));
    Assert.assertEquals(parts.get(0).getValues().get(0),
        UpdatePeriod.HOURLY.format().format(nowPlus1));
    Assert.assertEquals(parts.get(0).getValues().get(2),
        UpdatePeriod.HOURLY.format().format(nowMinus2));

    parts = client.getPartitionsByFilter(storageTableName,
        etPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey(etPart.getName())),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(0),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1),
        "default");

    client.dropPartition(cubeFact.getName(), c1, timeParts5, null,
        UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 3);
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertFalse(client.latestPartitionExists(cubeFact.getName(), c1,
        itPart.getName()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        etPart.getName()));
    parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1), "default");
    Assert.assertEquals(parts.get(0).getValues().get(2),
        UpdatePeriod.HOURLY.format().format(now));

    parts = client.getPartitionsByFilter(storageTableName,
        itPart.getName() + "='latest'");
    Assert.assertEquals(0, parts.size());

    parts = client.getPartitionsByFilter(storageTableName,
        etPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey(etPart.getName())),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(0),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(parts.get(0).getValues().get(1),
        "default");

    client.dropPartition(cubeFact.getName(), c1, timeParts3, nonTimeSpec,
        UpdatePeriod.HOURLY);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
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
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    timePartCols.add(getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    List<String> cubeNames = new ArrayList<String>();
    cubeNames.add(cubeName);

    CubeFactTable cubeFact = new CubeFactTable(cubeNames, factName,
        factColumns, updatePeriods, 100L);

    // create cube fact
    client.createCubeFactTable(cubeNames, factName, factColumns,
        updatePeriods, 100L, null, storageTables);

    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeName));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFact.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(
          factName, entry);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    // test partition
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    StoragePartitionDesc partSpec = new StoragePartitionDesc(cubeFact.getName(),
        timeParts, null, UpdatePeriod.HOURLY);
    client.addPartition(partSpec, c1);
    Assert.assertTrue(client.factPartitionExists(cubeFact.getName(), c1,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        cubeFact.getName(), c1);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);

    client.dropPartition(cubeFact.getName(), c1, timeParts, null,
        UpdatePeriod.HOURLY);
    Assert.assertFalse(client.factPartitionExists(cubeFact.getName(), c1,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
    Assert.assertFalse(client.latestPartitionExists(cubeFact.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
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
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    partCols.add(factPartColumns.get(0));
    timePartCols.add(getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    List<String> cubeNames = new ArrayList<String>();
    cubeNames.add(cubeName);

    CubeFactTable cubeFactWithParts = new CubeFactTable(cubeNames, factNameWithPart,
        factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(cubeNames, factNameWithPart, factColumns,
        updatePeriods, 0L, null, storageTables);

    Assert.assertTrue(client.tableExists(factNameWithPart));
    Table cubeTbl = client.getHiveTable(factNameWithPart);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeName));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFactWithParts.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(
          factNameWithPart, entry);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put(factPartColumns.get(0).getName(), "APAC");
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    // test partition
    StoragePartitionDesc sPartSpec = new StoragePartitionDesc(cubeFactWithParts.getName(),
        timeParts, partSpec, UpdatePeriod.HOURLY);
    client.addPartition(sPartSpec, c1);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithParts.getName(),
        c1,
        UpdatePeriod.HOURLY, timeParts, partSpec));
    Assert.assertTrue(client.latestPartitionExists(cubeFactWithParts.getName(),
        c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertFalse(client.latestPartitionExists(cubeFactWithParts.getName(),
        c1,
        factPartColumns.get(0).getName()));
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        cubeFactWithParts.getName(), c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));

    client.dropPartition(cubeFactWithParts.getName(), c1, timeParts, partSpec,
        UpdatePeriod.HOURLY);
    Assert.assertFalse(client.factPartitionExists(cubeFactWithParts.getName(), c1,
        UpdatePeriod.HOURLY, timeParts, partSpec));
    Assert.assertFalse(client.latestPartitionExists(cubeFactWithParts.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
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
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    FieldSchema testDtPart = new FieldSchema("mydate", "string", "date part");
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    partCols.add(testDtPart);
    partCols.add(factPartColumns.get(0));
    timePartCols.add(getDatePartitionKey());
    timePartCols.add(testDtPart.getName());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    List<String> cubeNames = new ArrayList<String>();
    cubeNames.add(cubeName);

    CubeFactTable cubeFactWithParts = new CubeFactTable(cubeNames, factNameWithPart,
        factColumns, updatePeriods);

    // create cube fact
    client.createCubeFactTable(cubeNames, factNameWithPart, factColumns,
        updatePeriods, 0L, null, storageTables);


    Assert.assertTrue(client.tableExists(factNameWithPart));
    Table cubeTbl = client.getHiveTable(factNameWithPart);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeName));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFactWithParts.equals(cubeFact2));

    // Assert for storage tables
    for (String entry :storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(
          factNameWithPart, entry);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    Calendar cal = new GregorianCalendar();
    cal.setTime(now);
    cal.add(Calendar.HOUR, -1);
    Date testDt = cal.getTime();
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put(factPartColumns.get(0).getName(), "APAC");
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    timeParts.put(testDtPart.getName(), testDt);
    // test partition
    StoragePartitionDesc sPartSpec = new StoragePartitionDesc(cubeFactWithParts.getName(),
        timeParts, partSpec, UpdatePeriod.HOURLY);
    client.addPartition(sPartSpec, c1);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithParts.getName(),
        c1,
        UpdatePeriod.HOURLY, timeParts, partSpec));
    Assert.assertTrue(client.latestPartitionExists(cubeFactWithParts.getName(),
        c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertTrue(client.latestPartitionExists(cubeFactWithParts.getName(),
        c1,
        testDtPart.getName()));
    Assert.assertFalse(client.latestPartitionExists(cubeFactWithParts.getName(),
        c1,
        factPartColumns.get(0).getName()));
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        cubeFactWithParts.getName(), c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 3);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));
    parts = client.getPartitionsByFilter(storageTableName,
        testDtPart.getName() + "='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey(testDtPart.getName())),
        UpdatePeriod.HOURLY.format().format(testDt));

    client.dropPartition(cubeFactWithParts.getName(), c1, timeParts, partSpec,
        UpdatePeriod.HOURLY);
    Assert.assertFalse(client.factPartitionExists(cubeFactWithParts.getName(), c1,
        UpdatePeriod.HOURLY, timeParts, partSpec));
    Assert.assertFalse(client.latestPartitionExists(cubeFactWithParts.getName(),
        c1,
        testDtPart.getName()));
    Assert.assertFalse(client.latestPartitionExists(cubeFactWithParts.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
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
    Set<UpdatePeriod> updates  = new HashSet<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    partCols.add(factPartColumns.get(0));
    timePartCols.add(getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(SequenceFileInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    updatePeriods.put(c1, updates);
    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    ArrayList<FieldSchema> partCols2 = new ArrayList<FieldSchema>();
    partCols2.add(getDatePartition());
    s2.setPartCols(partCols2);
    s2.setTimePartCols(timePartCols);
    updatePeriods.put(c2, updates);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);
    storageTables.put(c2, s2);

    List<String> cubeNames = new ArrayList<String>();
    cubeNames.add(cubeName);

    CubeFactTable cubeFactWithTwoStorages = new CubeFactTable(cubeNames, factName,
        factColumns, updatePeriods);


    client.createCubeFactTable(cubeNames, factName, factColumns,
        updatePeriods, 0L, null, storageTables);

    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    Assert.assertTrue(client.isFactTable(cubeTbl));
    Assert.assertTrue(client.isFactTableForCube(cubeTbl, cubeName));
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFactWithTwoStorages.equals(cubeFact2));

    // Assert for storage tables
    for (String entry : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getFactStorageTableName(
          factName, entry);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put(factPartColumns.get(0).getName(), "APAC");
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    // test partition
    StoragePartitionDesc sPartSpec = new StoragePartitionDesc(cubeFactWithTwoStorages.getName(),
        timeParts, partSpec, UpdatePeriod.HOURLY);
    client.addPartition(sPartSpec, c1);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithTwoStorages.getName(),
      c1,
      UpdatePeriod.HOURLY, timeParts, partSpec));
    Assert.assertTrue(client.latestPartitionExists(cubeFactWithTwoStorages.getName(),
        c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        cubeFactWithTwoStorages.getName(), c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(SequenceFileInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));

    StoragePartitionDesc sPartSpec2 = new StoragePartitionDesc(cubeFactWithTwoStorages.getName(),
        timeParts, null, UpdatePeriod.HOURLY);
    client.addPartition(sPartSpec2, c2);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithTwoStorages.getName(),
        c2,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
    Assert.assertTrue(client.latestPartitionExists(cubeFactWithTwoStorages.getName(),
        c2,
      TestCubeMetastoreClient.getDatePartitionKey()));
    String storageTableName2 = MetastoreUtil.getFactStorageTableName(
        cubeFactWithTwoStorages.getName(), c2);
    Assert.assertEquals(client.getAllParts(storageTableName2).size(), 2);
    parts = client.getPartitionsByFilter(storageTableName2,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));


    client.dropPartition(cubeFactWithTwoStorages.getName(), c1, timeParts, partSpec,
        UpdatePeriod.HOURLY);
    Assert.assertFalse(client.factPartitionExists(cubeFactWithTwoStorages.getName(), c1,
        UpdatePeriod.HOURLY, timeParts, partSpec));
    Assert.assertFalse(client.latestPartitionExists(cubeFactWithTwoStorages.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);

    client.dropPartition(cubeFactWithTwoStorages.getName(),
        c2, timeParts, null, UpdatePeriod.HOURLY);
    Assert.assertFalse(client.factPartitionExists(
        cubeFactWithTwoStorages.getName(), c2,
        UpdatePeriod.HOURLY, timeParts, new HashMap<String, String>()));
    Assert.assertFalse(client.latestPartitionExists(
        cubeFactWithTwoStorages.getName(), c2,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertEquals(client.getAllParts(storageTableName2).size(), 0);
  }

  @Test
  public void testCubeDimWithWeight() throws Exception {
    String dimName = "statetable";

    List<FieldSchema> dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "state id"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("capital", "string", "field2"));
    dimColumns.add(new FieldSchema("countryid", "int", "country id"));

    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();
    dimensionReferences.put("countryid", Arrays.asList(new TableReference("countrytable", "id")));

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    timePartCols.add(getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    CubeDimensionTable cubeDim = new CubeDimensionTable(dimName,
        dimColumns, 100L, dumpPeriods, dimensionReferences);
    client.createCubeDimensionTable(dimName, dimColumns, 100L,
        dimensionReferences, dumpPeriods, null, storageTables);
    Assert.assertTrue(client.tableExists(dimName));
    Table cubeTbl = client.getHiveTable(dimName);
    Assert.assertTrue(client.isDimensionTable(cubeTbl));
    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    Assert.assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    for (String storage : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getStorageTableName(dimName,
          Storage.getPrefix(storage));
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    // test partition
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    StoragePartitionDesc sPartSpec = new StoragePartitionDesc(cubeDim.getName(),
        timeParts, null, UpdatePeriod.HOURLY);
    client.addPartition(sPartSpec, c1);
    Assert.assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timeParts));
    Assert.assertTrue(client.latestPartitionExists(cubeDim.getName(),
        c1, TestCubeMetastoreClient.getDatePartitionKey()));
    String storageTableName = MetastoreUtil.getDimStorageTableName(
        dimName, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));

    client.dropPartition(cubeDim.getName(), c1, timeParts, null,
        UpdatePeriod.HOURLY);
    Assert.assertFalse(client.dimPartitionExists(cubeDim.getName(), c1,
        timeParts));
    Assert.assertFalse(client.latestPartitionExists(cubeDim.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
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

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    timePartCols.add(getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    CubeDimensionTable cubeDim = new CubeDimensionTable(dimName,
        dimColumns, 0L, dumpPeriods, dimensionReferences);
    client.createCubeDimensionTable(dimName, dimColumns, 0L,
        dimensionReferences, dumpPeriods, null, storageTables);

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
    for (String storage : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimName,
          storage);
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    FieldSchema newcol = new FieldSchema("newcol", "int", "new col for part");

    // test partition
    Map<String, Date> timeParts = new HashMap<String, Date>();
    timeParts.put(TestCubeMetastoreClient.getDatePartitionKey(), now);
    StoragePartitionDesc sPartSpec = new StoragePartitionDesc(cubeDim.getName(),
        timeParts, null, UpdatePeriod.HOURLY);
    client.addPartition(sPartSpec, c1);
    Assert.assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timeParts));
    Assert.assertTrue(client.latestPartitionExists(cubeDim.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    String storageTableName = MetastoreUtil.getDimStorageTableName(
        dimName, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);
    List<Partition> parts = client.getPartitionsByFilter(storageTableName,
        "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertFalse(parts.get(0).getCols().contains(newcol));
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));

    // Partition with different schema
    cubeDim.alterColumn(newcol);
    client.alterCubeDimensionTable(cubeDim.getName(), cubeDim);

    Map<String, Date> timeParts2 = new HashMap<String, Date>();
    timeParts2.put(TestCubeMetastoreClient.getDatePartitionKey(), nowPlus1);
    StoragePartitionDesc sPartSpec2 = new StoragePartitionDesc(cubeDim.getName(),
        timeParts2, null, UpdatePeriod.HOURLY);
    sPartSpec2.setInputFormat(SequenceFileInputFormat.class.getCanonicalName());
    sPartSpec2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    client.addPartition(sPartSpec2, c1);
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 3);
    Assert.assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timeParts));
    Assert.assertTrue(client.dimPartitionExists(cubeDim.getName(), c1, timeParts2));
    Assert.assertTrue(client.latestPartitionExists(cubeDim.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(SequenceFileInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertTrue(parts.get(0).getCols().contains(newcol));
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(nowPlus1));

    client.dropPartition(cubeDim.getName(), c1, timeParts2, null,
        UpdatePeriod.HOURLY);
    Assert.assertTrue(client.dimPartitionExists(cubeDim.getName(), c1,
        timeParts));
    Assert.assertFalse(client.dimPartitionExists(cubeDim.getName(), c1,
        timeParts2));
    Assert.assertTrue(client.latestPartitionExists(cubeDim.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    parts = client.getPartitionsByFilter(storageTableName, "dt='latest'");
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals(TextInputFormat.class.getCanonicalName(),
        parts.get(0).getInputFormatClass().getCanonicalName());
    Assert.assertEquals(parts.get(0).getParameters().get(
        MetastoreUtil.getLatestPartTimestampKey("dt")),
        UpdatePeriod.HOURLY.format().format(now));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 2);

    client.dropPartition(cubeDim.getName(), c1, timeParts, null,
        UpdatePeriod.HOURLY);
    Assert.assertFalse(client.dimPartitionExists(cubeDim.getName(), c1,
        timeParts));
    Assert.assertFalse(client.dimPartitionExists(cubeDim.getName(), c1,
        timeParts2));
    Assert.assertFalse(client.latestPartitionExists(cubeDim.getName(), c1,
        TestCubeMetastoreClient.getDatePartitionKey()));
    Assert.assertEquals(client.getAllParts(storageTableName).size(), 0);
  }

  @Test
  public void testAlterDim() throws Exception {
    String dimName = "test_alter_dim";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("zipcode", "int", "code"));

    Map<String, List<TableReference>> dimensionReferences =
      new HashMap<String, List<TableReference>>();

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    timePartCols.add(getDatePartitionKey());
    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    s1.setPartCols(partCols);
    s1.setTimePartCols(timePartCols);
    dumpPeriods.put(c1, UpdatePeriod.HOURLY);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    client.createCubeDimensionTable(dimName, dimColumns, 100L,
        dimensionReferences, dumpPeriods, null, storageTables);

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
    StorageTableDesc s2 = new StorageTableDesc();
    s2.setInputFormat(TextInputFormat.class.getCanonicalName());
    s2.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    client.addStorage(dimTable, c2, null, s2);
    client.addStorage(dimTable, c3, UpdatePeriod.DAILY, s1);
    Assert.assertTrue(client.tableExists(MetastoreUtil.getDimStorageTableName(
        dimName, c2)));
    Assert.assertTrue(client.tableExists(MetastoreUtil.getDimStorageTableName(
        dimName, c3)));
    Assert.assertFalse(dimTable.hasStorageSnapshots("C2"));
    Assert.assertTrue(dimTable.hasStorageSnapshots("C3"));
    client.dropStorageFromDim(dimName, "C1");
    Assert.assertFalse(client.tableExists(MetastoreUtil.getDimStorageTableName(
        dimName, c1)));
    client.dropDimension(dimName, true);
    Assert.assertFalse(client.tableExists(MetastoreUtil.getDimStorageTableName(
        dimName, c2)));
    Assert.assertFalse(client.tableExists(MetastoreUtil.getDimStorageTableName(
        dimName, c3)));
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
    Set<String> storageNames = new HashSet<String>();

    StorageTableDesc s1 = new StorageTableDesc();
    s1.setInputFormat(TextInputFormat.class.getCanonicalName());
    s1.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    storageNames.add(c1);

    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    storageTables.put(c1, s1);

    CubeDimensionTable cubeDim = new CubeDimensionTable(dimName,
        dimColumns, 0L, storageNames, dimensionReferences);

    client.createCubeDimensionTable(dimName, dimColumns, 0L,
        dimensionReferences, storageNames, null, storageTables);


    Assert.assertTrue(client.tableExists(dimName));
    Table cubeTbl = client.getHiveTable(dimName);
    Assert.assertTrue(client.isDimensionTable(cubeTbl));
    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    Assert.assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    for (String storageName : storageTables.keySet()) {
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimName,
          storageName);
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

    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(getDatePartition());
    timePartCols.add(getDatePartitionKey());
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

    CubeDimensionTable cubeDim = new CubeDimensionTable(dimName,
        dimColumns, 0L, dumpPeriods, dimensionReferences);
    client.createCubeDimensionTable(dimName, dimColumns, 0L,
        dimensionReferences, dumpPeriods, null, storageTables);

    Assert.assertTrue(client.tableExists(dimName));
    Table cubeTbl = client.getHiveTable(dimName);
    Assert.assertTrue(client.isDimensionTable(cubeTbl));
    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    Assert.assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    String storageTableName1 = MetastoreUtil.getDimStorageTableName(dimName, c1);
    Assert.assertTrue(client.tableExists(storageTableName1));
    String storageTableName2 = MetastoreUtil.getDimStorageTableName(dimName, c2);
    Assert.assertTrue(client.tableExists(storageTableName2));
    Assert.assertTrue(!client.getHiveTable(storageTableName2).isPartitioned());
  }

  @Test
  public void testCaching() throws HiveException {
    client = CubeMetastoreClient.getInstance(conf);
    CubeMetastoreClient client2 = CubeMetastoreClient.getInstance(
        new HiveConf(TestCubeMetastoreClient.class));
    Assert.assertEquals(3, client.getAllCubes().size());
    Assert.assertEquals(3, client2.getAllCubes().size());

    defineCube("testcache1", "testcache2");
    client.createCube("testcache1", cubeMeasures, cubeDimensions);
    client.createCube("testcache2", cubeMeasures, cubeDimensions, cubeProperties);
    Assert.assertNotNull(client.getCube("testcache1"));
    Assert.assertNotNull(client2.getCube("testcache1"));
    Assert.assertEquals(5, client.getAllCubes().size());
    Assert.assertEquals(5, client2.getAllCubes().size());

    client2 = CubeMetastoreClient.getInstance(conf);
    Assert.assertEquals(5, client.getAllCubes().size());
    Assert.assertEquals(5, client2.getAllCubes().size());

    conf.setBoolean(MetastoreConstants.METASTORE_ENABLE_CACHING, false);
    client = CubeMetastoreClient.getInstance(conf);
    client2 = CubeMetastoreClient.getInstance(conf);
    Assert.assertEquals(5, client.getAllCubes().size());
    Assert.assertEquals(5, client2.getAllCubes().size());
    defineCube("testcache3", "testcache4");
    client.createCube("testcache3", cubeMeasures, cubeDimensions);
    client.createCube("testcache4", cubeMeasures, cubeDimensions, cubeProperties);
    Assert.assertNotNull(client.getCube("testcache3"));
    Assert.assertNotNull(client2.getCube("testcache3"));
    Assert.assertEquals(7, client.getAllCubes().size());
    Assert.assertEquals(7, client2.getAllCubes().size());
    conf.setBoolean(MetastoreConstants.METASTORE_ENABLE_CACHING, true);
    client = CubeMetastoreClient.getInstance(conf);
  }
}
