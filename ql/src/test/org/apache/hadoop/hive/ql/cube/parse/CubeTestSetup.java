package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.BaseDimension;
import org.apache.hadoop.hive.ql.cube.metadata.ColumnMeasure;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
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
import org.apache.hadoop.mapred.TextInputFormat;

public class CubeTestSetup {

  private Cube cube;
  private Set<CubeMeasure> cubeMeasures;
  private Set<CubeDimension> cubeDimensions;
  public static final String TEST_CUBE_NAME= "testCube";
  public static Date now;
  public static Date twodaysBack;
  public static Date twoMonthsBack;
  private static boolean zerothHour;

  static {
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
  }

  public static boolean isZerothHour() {
    return zerothHour;
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
        TEST_CUBE_NAME), Storage.getDatePartitionKey());
    cube = new Cube(TEST_CUBE_NAME, cubeMeasures, cubeDimensions, cubeProperties);
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

    Map<Storage, List<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, List<UpdatePeriod>>();
    List<UpdatePeriod> updates  = new ArrayList<UpdatePeriod>();
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

    Map<Storage, List<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, List<UpdatePeriod>>();
    List<UpdatePeriod> updates  = new ArrayList<UpdatePeriod>();
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

    Map<Storage, List<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, List<UpdatePeriod>>();
    List<UpdatePeriod> updates  = new ArrayList<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage.addToPartCols(Storage.getDatePartition());
    storageAggregatePeriods.put(hdfsStorage, updates);

    // create cube fact
    client.createCubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageAggregatePeriods, 0L, null);
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

    Map<Storage, List<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, List<UpdatePeriod>>();
    List<UpdatePeriod> updates  = new ArrayList<UpdatePeriod>();
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
    List<UpdatePeriod> updates  = new ArrayList<UpdatePeriod>();
    updates.add(UpdatePeriod.MINUTELY);
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    Map<String, List<UpdatePeriod>> storageUpdatePeriods =
        new HashMap<String, List<UpdatePeriod>>();

    Set<Storage> storages = new HashSet<Storage>();
    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage.addToPartCols(Storage.getDatePartition());
    storages.add(hdfsStorage);
    storageUpdatePeriods.put(hdfsStorage.getName(), updates);

    // create cube fact summary1
    Map<String, String> properties = new HashMap<String, String>();
    String validColumns = commonCols.toString() + ",dim1";
    properties.put(MetastoreUtil.getValidColumnsKey(factName),
        validColumns);
    CubeFactTable fact1 = new CubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageUpdatePeriods, 10L, properties);
    client.createCubeTable(fact1, storages);

    // create summary2 - same schema, different valid columns
    factName = "summary2";
    storages = new HashSet<Storage>();
    hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage.addToPartCols(Storage.getDatePartition());
    storages.add(hdfsStorage);
    storageUpdatePeriods.put(hdfsStorage.getName(), updates);
    properties = new HashMap<String, String>();
    validColumns = commonCols.toString() + ",dim1,dim2";
    properties.put(MetastoreUtil.getValidColumnsKey(factName),
        validColumns);
    CubeFactTable fact2 = new CubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageUpdatePeriods, 20L, properties);
    client.createCubeTable(fact2, storages);

    factName = "summary3";
    storages = new HashSet<Storage>();
    hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorage.addToPartCols(Storage.getDatePartition());
    storages.add(hdfsStorage);
    storageUpdatePeriods.put(hdfsStorage.getName(), updates);
    properties = new HashMap<String, String>();
    validColumns = commonCols.toString() + ",dim1,dim2,cityid";
    properties.put(MetastoreUtil.getValidColumnsKey(factName),
        validColumns);
    CubeFactTable fact3 = new CubeFactTable(TEST_CUBE_NAME, factName, factColumns,
        storageUpdatePeriods, 30L, properties);
    client.createCubeTable(fact3, storages);
  }
  
  public static void  printQueryAST(String query, String label) throws ParseException {
    System.out.println("--" + label + "--AST--");
    System.out.println("--query- " + query);
    HQLParser.printAST(HQLParser.parseHQL(query));
  }

}
