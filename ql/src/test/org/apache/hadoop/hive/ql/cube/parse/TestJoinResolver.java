package org.apache.hadoop.hive.ql.cube.parse;

import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.*;
import org.apache.hadoop.hive.ql.cube.metadata.SchemaGraph;
import static org.apache.hadoop.hive.ql.cube.metadata.SchemaGraph.TableRelationship;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
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
    SessionState.start(hconf);
    setup = new CubeTestSetup();
    setup.createSources(hconf, TestJoinResolver.class.getSimpleName());
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
    hconf.setBoolean(JoinResolver.DISABLE_AUTO_JOINS, false);
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
    assertEquals("id", dim4edge.getToColumn());
    assertEquals(metastore.getDimensionTable("testdim4"), dim4edge.getToTable());
    assertEquals("testdim4id", dim4edge.getFromColumn());
    assertEquals(metastore.getDimensionTable("testdim3"), dim4edge.getFromTable());
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
    CubeQueryContext rewrittenQuery = driver.rewrite(query);
    String hql = rewrittenQuery.toHQL();
    System.out.println("auto join HQL:" + hql);
    System.out.println("@@Resolved join chain:[" + rewrittenQuery.getAutoResolvedJoinChain()+ "]");
    Set<String> expectedClauses = new HashSet<String>();
    expectedClauses.add("citytable on testcube.cityid = citytable.id");
    expectedClauses.add("testdim2 on testcube.dim2 = testdim2.id");
    expectedClauses.add("testdim3 on testdim2.testdim3id = testdim3.id");
    expectedClauses.add("testdim4 on testdim3.testdim4id = testdim4.id");

    Set<String> actualClauses = new HashSet<String>();
    for (String clause : StringUtils.splitByWholeSeparator(rewrittenQuery.getAutoResolvedJoinChain(), "join")) {
      if (StringUtils.isNotBlank(clause))  {
        actualClauses.add(clause.trim());
      }
    }
    System.out.println("Expected" + expectedClauses);
    System.out.println("Actual" + actualClauses);
    assertEquals(actualClauses, expectedClauses);

    //Test 2  Dim only query
    expectedClauses.clear();
    actualClauses.clear();
    String dimOnlyQuery = "select testDim2.name, testDim4.name FROM testDim2 where "
      + twoDaysRange;
    rewrittenQuery = driver.rewrite(dimOnlyQuery);
    hql = rewrittenQuery.toHQL();
    System.out.println("auto join HQL:" + hql);
    System.out.println("@@Resolved join chain:[" + rewrittenQuery.getAutoResolvedJoinChain()+ "]");
    expectedClauses.add("testdim3 on testdim2.testdim3id = testdim3.id");
    expectedClauses.add("testdim4 on testdim3.testdim4id = testdim4.id");
    for (String clause : StringUtils.splitByWholeSeparator(rewrittenQuery.getAutoResolvedJoinChain(), "join")) {
      if (StringUtils.isNotBlank(clause))  {
        actualClauses.add(clause.trim());
      }
    }
    System.out.println("Expected" + expectedClauses);
    System.out.println("Actual" + actualClauses);
    assertEquals(actualClauses, expectedClauses);

    //Test 3 Dim only query should throw error
    String errDimOnlyQuery = "select citytable.id, testDim4.name FROM citytable where "
      + twoDaysRange;
    try {
      hql = rewrite(driver, errDimOnlyQuery);
      fail("dim only query should throw error");
    } catch (SemanticException exc) {
    }
  }

  @Test
  public void testPartialJoinResolver() throws Exception {
    String query = "SELECT citytable.name, testDim4.name, msr2 " +
      "FROM testCube join citytable ON citytable.name = 'FOOBAR'" +
      " join testDim4 on testDim4.name='TESTDIM4NAME'" +
      " WHERE " + twoDaysRange;
    CubeQueryContext rewrittenQuery = driver.rewrite(query);
    String hql = rewrittenQuery.toHQL();
    String resolvedClause = rewrittenQuery.getAutoResolvedJoinChain();
    System.out.println("@@resolved join chain " + resolvedClause);
    Set<String> expectedClauses = new HashSet<String>();
    expectedClauses.add("citytable on testcube.cityid = citytable.id and ((( citytable  .  name ) =  'FOOBAR' )) and (citytable.dt = 'latest')");
    expectedClauses.add("testdim4 on testdim3.testdim4id = testdim4.id and ((( testdim4  .  name ) =  'TESTDIM4NAME' )) and (testdim4.dt = 'latest')");
    expectedClauses.add("testdim3 on testdim2.testdim3id = testdim3.id");
    expectedClauses.add("testdim2 on testcube.dim2 = testdim2.id");

    Set<String> actualClauses = new HashSet<String>();
    for (String clause : StringUtils.splitByWholeSeparator(rewrittenQuery.getAutoResolvedJoinChain(), "join")) {
      if (StringUtils.isNotBlank(clause))  {
        actualClauses.add(clause.trim());
      }
    }
    System.out.println("Expected" + expectedClauses);
    System.out.println("Actual" + actualClauses);
    assertEquals(actualClauses, expectedClauses);
  }

  @Test
  public void testJoinNotRequired() throws Exception {
    String query = "SELECT msr2 FROM testCube WHERE " + twoDaysRange;
    CubeQueryContext ctx = driver.rewrite(query);
    String hql = ctx.toHQL();
    String joinClause = ctx.getAutoResolvedJoinChain();
    System.out.println("@Resolved join clause " + joinClause);
    assertTrue(joinClause == null || joinClause.isEmpty());
  }

  @Test
  public void testJoinWithoutCondition() throws Exception {
    String query = "SELECT citytable.name, msr2 FROM testCube WHERE " + twoDaysRange;
    CubeQueryContext ctx = driver.rewrite(query);
    String hql = ctx.toHQL();
    String joinClause = ctx.getAutoResolvedJoinChain();
    System.out.println("@Resolved join clause " + joinClause);
    assertEquals("join citytable on testcube.cityid = citytable.id", joinClause.trim());
  }

  @Test
  public void testJoinTypeConf() throws Exception {
    hconf.set(JoinResolver.JOIN_TYPE_KEY, "LEFTOUTER");
    System.out.println("@@Set join type to " + hconf.get(JoinResolver.JOIN_TYPE_KEY));
    driver = new CubeQueryRewriter(hconf);
    String query = "select citytable.name, msr2 FROM testCube WHERE " + twoDaysRange;
    CubeQueryContext ctx = driver.rewrite(query);
    String hql = ctx.toHQL();
    System.out.println("@@Resolved join clause - " + ctx.getAutoResolvedJoinChain());
    assertEquals("left outer join citytable on testcube.cityid = citytable.id",
      ctx.getAutoResolvedJoinChain().trim());

    hconf.set(JoinResolver.JOIN_TYPE_KEY, "FULLOUTER");
    System.out.println("@@Set join type to " + hconf.get(JoinResolver.JOIN_TYPE_KEY));
    driver = new CubeQueryRewriter(hconf);
    ctx = driver.rewrite(query);
    System.out.println("@@Resolved join clause - "+ ctx.getAutoResolvedJoinChain());
    assertEquals("full outer join citytable on testcube.cityid = citytable.id",
      ctx.getAutoResolvedJoinChain().trim());
  }
}
