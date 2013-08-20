package org.apache.hadoop.hive.ql.cube.parse;


import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.*;

import java.util.List;
import static org.junit.Assert.*;

import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.*;

public class TestTimeRangeExtractor {
  private CubeMetastoreClient metastore;
  private static CubeTestSetup setup;
  private static HiveConf hconf = new HiveConf(TestTimeRangeExtractor.class);
  private CubeQueryRewriter driver;
  private CubeQueryContext cubeql;
  private String dateNow;
  private String dateTwoDaysBack;


  @BeforeClass
  public static void setup() throws Exception {
    setup = new CubeTestSetup();
    setup.createSources(hconf, TestJoinResolver.class.getSimpleName());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    setup.dropSources(hconf);
  }

  @Before
  public void setupInstance() throws Exception {
    this.metastore = CubeMetastoreClient.getInstance(hconf);
    driver = new CubeQueryRewriter(hconf);
    dateTwoDaysBack = getDateUptoHours(twodaysBack);
    dateNow = getDateUptoHours(now);

  }

  @After
  public void closeInstance() throws  Exception {
  }

  public static String rewrite(CubeQueryRewriter driver, String query)
    throws SemanticException, ParseException {
    CubeQueryContext rewrittenQuery = driver.rewrite(query);
    return rewrittenQuery.toHQL();
  }

  @Test
  public void testTimeRangeValidation() throws Exception {
    String timeRange2 = " time_range_in('dt', '" + dateNow + "','" + dateTwoDaysBack + "')";
    try {
      // this should throw exception because from date is after to date
      CubeQueryContext rewrittenQuery = driver.rewrite("SELECT cityid, testCube.msr2 from" +
        " testCube where " + timeRange2);
      fail("Should not reach here");
    } catch (SemanticException exc) {
      exc.printStackTrace();
      assertNotNull(exc);
    }
  }

  @Test
  public void testNoNPE() throws Exception {
    // GRILL-38 NPE in extracting time range
    String timeRange = " time_range_in('dt', '" + dateTwoDaysBack + "','" + dateNow + "')";
    String q1 = "SELECT cityid, testCube.msr2 from testCube where " + timeRange + " AND cityid IS NULL";
    rewrite(driver, q1);
    q1 = "SELECT cityid, testCube.msr2 from testCube where cityid IS NULL AND " + timeRange;
    rewrite(driver, q1);
  }

  @Test
  public void testTimeRangeASTPosition() throws Exception {
    // check that time range can be any child of AND
    String timeRange = " time_range_in('dt', '" + dateTwoDaysBack + "','" + dateNow + "')";
    String q1 = "SELECT cityid, testCube.msr2 from testCube where " + timeRange + " AND cityid=1";
    CubeQueryContext cubeql = driver.rewrite(q1);
    String hql = cubeql.toHQL();
  }

  @Test
  public void testPartitionColNameExtract() throws Exception  {
    String q2 = "SELECT cityid, testCube.msr3 from testCube where cityid=1 AND " +
      " time_range_in('dt', '" + dateTwoDaysBack + "','" + dateNow + "')";
    CubeQueryContext cubeql = driver.rewrite(q2);
    String hql = cubeql.toHQL();
    // Check that column name in time range is extracted properly
    TimeRange range = cubeql.getTimeRanges().get(0);
    assertNotNull(range);
    assertEquals("Time dimension should be " + Storage.getDatePartitionKey(),
      Storage.getDatePartitionKey(),
      range.getPartitionColumn());
  }

  @Test
  public void testTimeRangeWithinTimeRange() throws Exception {
    System.out.println("###");
    String dateTwoDaysBack = getDateUptoHours(twodaysBack);
    String dateNow = getDateUptoHours(now);
    // time range within time range
    String q3 = "SELECT cityid, testCube.msr3 FROM testCube where cityid=1 AND  (time_range_in('dt', '" + dateTwoDaysBack
      + "','" +dateNow+ "',  "
      // Another time range inside the above function
      + " time_range_in('dt', '" + dateTwoDaysBack + "', '" +  dateNow + "'))" +
      // Time range as sibling of the first time range
      " AND " + " time_range_in('dt', '" + dateTwoDaysBack + "', '" +  dateNow + "'))";
    CubeQueryContext cubeql = driver.rewrite(q3);
    String hql = cubeql.toHQL();

    List<TimeRange> ranges = cubeql.getTimeRanges();
    assertEquals(2, ranges.size());

    TimeRange first = ranges.get(0);
    assertNotNull(first);
    assertNotNull(first.getChild());
    assertEquals(dateTwoDaysBack, getDateUptoHours(first.getFromDate()));
    assertEquals(dateNow, getDateUptoHours(first.getToDate()));

    TimeRange firstChild = first.getChild();
    assertNull(firstChild.getChild());
    assertEquals("dt", firstChild.getPartitionColumn());

    TimeRange second = ranges.get(1);
    assertNotNull(second);
    assertEquals("dt", second.getPartitionColumn());
    assertEquals(dateTwoDaysBack, getDateUptoHours(second.getFromDate()));
    assertEquals(dateNow, getDateUptoHours(second.getToDate()));
    assertNull(second.getChild());
  }
}
