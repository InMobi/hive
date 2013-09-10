package org.apache.hadoop.hive.ql.cube.parse;

import java.util.*;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.AbstractCubeTable;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.parse.JoinResolver.SchemaGraph;
import org.apache.hadoop.hive.ql.cube.parse.JoinResolver.TableRelationship;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.*;

import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.*;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.twoDaysRange;
import static org.junit.Assert.*;

public class TestJoinResolver {
  
  private  static CubeTestSetup setup;
  private  static HiveConf hconf = new HiveConf(TestJoinResolver.class);
  private CubeQueryRewriter driver;
  private String dateTwoDaysBack;
  private String dateNow;
  private CubeMetastoreClient metastore;


  @BeforeClass
  public static void setup() throws Exception {
    setup = new CubeTestSetup();
    setup.createSources(hconf, TestJoinResolver.class.getSimpleName());
    hconf.setBoolean(JoinResolver.DISABLE_AUTO_JOINS, false);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    //setup.dropSources(hconf);
  }

  @Before
  public void setupInstance() throws Exception {
    driver = new CubeQueryRewriter(hconf);
    dateTwoDaysBack = getDateUptoHours(twodaysBack);
    dateNow = getDateUptoHours(now);
    this.metastore = CubeMetastoreClient.getInstance(hconf);
  }

  @After
  public void closeInstance() throws  Exception {
  }

  // testBuildGraph - graph correctness
  @Test 
  public void testBuildGraph() throws Exception {
    SchemaGraph schemaGraph = new SchemaGraph(metastore);
    schemaGraph.buildSchemaGraph();
    Cube cube = metastore.getCube(CubeTestSetup.TEST_CUBE_NAME);
    Map<AbstractCubeTable, Set<TableRelationship>> graph = schemaGraph.getCubeGraph(cube);
    printGraph(graph);
    assertNotNull(graph);
    
    // Let's do some lookups
    Set<TableRelationship> dim4Edges = graph.get(metastore.getDimensionTable("testdim4"));
    assertNotNull(dim4Edges);
    assertEquals(1, dim4Edges.size());
    
    List<TableRelationship> edges = new ArrayList<TableRelationship>(dim4Edges);
    TableRelationship dim4edge = edges.get(0);
    assertEquals("id", dim4edge.toColumn);
    assertEquals(metastore.getDimensionTable("testdim4"), dim4edge.toTable);
    assertEquals("testdim4id", dim4edge.fromColumn);
    assertEquals(metastore.getDimensionTable("testdim3"), dim4edge.fromTable);
  }
  
  @Test
  public void testFindChain() throws Exception {
    SchemaGraph schemaGraph = new SchemaGraph(metastore);
    schemaGraph.buildSchemaGraph();
    Cube cube = metastore.getCube(CubeTestSetup.TEST_CUBE_NAME);
    printGraph(schemaGraph.getCubeGraph(cube));

    CubeDimensionTable testDim4 = metastore.getDimensionTable("testdim4");
    List<TableRelationship> chain = new ArrayList<TableRelationship>();
    assertTrue(schemaGraph.findJoinChain(testDim4, cube, chain));
    System.out.println(chain);
    
    chain.clear();
    CubeDimensionTable cityTable = metastore.getDimensionTable("citytable");
    assertTrue(schemaGraph.findJoinChain(cityTable, cube, chain));
    System.out.println("City -> cube chain: " + chain);
    assertEquals(1, chain.size());
    
    // find chains between dimensions
    chain.clear();
    CubeDimensionTable stateTable = metastore.getDimensionTable("statetable");
    assertTrue(schemaGraph.findJoinChain(stateTable, cityTable, chain));
    System.out.println("Dim chain state -> city : " + chain);
    assertEquals(1, chain.size());

    chain.clear();
    assertTrue(schemaGraph.findJoinChain(cityTable, stateTable, chain));
    System.out.println("Dim chain city -> state: " + chain);
    assertEquals(1, chain.size());

    chain.clear();
    CubeDimensionTable dim2 = metastore.getDimensionTable("testdim2");
    assertTrue(schemaGraph.findJoinChain(testDim4, dim2, chain));
    System.out.println("Dim chain testdim4 -> testdim2 : " + chain);
    assertEquals(2, chain.size());

    chain.clear();
    assertTrue(schemaGraph.findJoinChain(dim2, testDim4, chain));
    assertEquals(2, chain.size());
    System.out.println("Dim chain testdim2 -> testdim4 : " + chain);
    
    chain.clear();
    boolean foundchain = schemaGraph.findJoinChain(testDim4, cityTable, chain);
    System.out.println("Dim chain testdim4 -> city table: " + chain);
    assertFalse(foundchain);
    assertFalse(schemaGraph.findJoinChain(cityTable, testDim4, chain));
  }
  
  private void printGraph(Map<AbstractCubeTable, Set<TableRelationship>> graph) {
    System.out.println("--Graph-Nodes=" + graph.size());
    for (AbstractCubeTable tab : graph.keySet()) {
      System.out.println(tab.getName() + "::" + graph.get(tab));
    }
  }

  @Test
  public void testAutoJoinResolver() throws Exception {
    //Test 1 Cube + dim
    String query = "select citytable.name, testDim2.name, testDim4.name, msr2 from testCube where "
      + twoDaysRange;
    String hql = rewrite(driver, query);
    System.out.println("auto join HQL:" + hql);

    //Test 2  Dim only query
    String dimOnlyQuery = "select testDim2.name, testDim4.name FROM testDim2 where "
      + twoDaysRange;
    hql = rewrite(driver, dimOnlyQuery);
    System.out.println("auto join HQL:" + hql);

    //Test 3 Dim only query should throw error
    String errDimOnlyQuery = "select citytable.id, testDim4.name FROM citytable where "
      + twoDaysRange;
    try {
      hql = rewrite(driver, errDimOnlyQuery);
      Assert.assertTrue("dim only query should throw error", false);
    } catch (SemanticException exc) {
      Assert.assertTrue(true);
    }
  }
}
