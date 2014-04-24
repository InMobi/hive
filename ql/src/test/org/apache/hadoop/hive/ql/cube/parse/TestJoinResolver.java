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


import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.getDateUptoHours;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.now;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.rewrite;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.twoDaysRange;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.twodaysBack;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.AbstractCubeTable;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.SchemaGraph;
import org.apache.hadoop.hive.ql.cube.metadata.SchemaGraph.TableRelationship;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
    hconf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
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
    System.out.println("testAutoJoinResolverauto join HQL:" + hql);
    System.out.println("testAutoJoinResolver@@Resolved join chain:[" + rewrittenQuery.getAutoResolvedJoinChain()+ "]");
    Set<String> expectedClauses = new HashSet<String>();
    expectedClauses.add("c1_citytable citytable on testcube.cityid = citytable.id and (citytable.dt = 'latest')");
    expectedClauses.add("c1_testdim2 testdim2 on testcube.dim2 = testdim2.id and (testdim2.dt = 'latest')");
    expectedClauses.add("c1_testdim3 testdim3 on testdim2.testdim3id = testdim3.id and (testdim3.dt = 'latest')");
    expectedClauses.add("c1_testdim4 testdim4 on testdim3.testdim4id = testdim4.id and (testdim4.dt = 'latest')");

    Set<String> actualClauses = new HashSet<String>();
    for (String clause : StringUtils.splitByWholeSeparator(rewrittenQuery.getAutoResolvedJoinChain(), "join")) {
      if (StringUtils.isNotBlank(clause))  {
        actualClauses.add(clause.trim());
      }
    }
    System.out.println("testAutoJoinResolverExpected1" + expectedClauses);
    System.out.println("testAutoJoinResolverActual1" + actualClauses);
    assertEquals(expectedClauses, actualClauses);

    //Test 2  Dim only query
    expectedClauses.clear();
    actualClauses.clear();
    String dimOnlyQuery = "select testDim2.name, testDim4.name FROM testDim2 where "
      + twoDaysRange;
    rewrittenQuery = driver.rewrite(dimOnlyQuery);
    hql = rewrittenQuery.toHQL();
    System.out.println("testAutoJoinResolverauto join HQL:" + hql);
    System.out.println("testAutoJoinResolver@@Resolved join chain:[" + rewrittenQuery.getAutoResolvedJoinChain()+ "]");
    expectedClauses.add("c1_testdim3 testdim3 on testdim2.testdim3id = testdim3.id and (testdim3.dt = 'latest')");
    expectedClauses.add("c1_testdim4 testdim4 on testdim3.testdim4id = testdim4.id and (testdim4.dt = 'latest')");
    for (String clause : StringUtils.splitByWholeSeparator(rewrittenQuery.getAutoResolvedJoinChain(), "join")) {
      if (StringUtils.isNotBlank(clause))  {
        actualClauses.add(clause.trim());
      }
    }
    System.out.println("testAutoJoinResolverExpected2" + expectedClauses);
    System.out.println("testAutoJoinResolverActual2" + actualClauses);
    assertEquals(expectedClauses, actualClauses);

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
    expectedClauses.add("c1_citytable citytable on testcube.cityid = citytable.id and ((( citytable  .  name ) =  'FOOBAR' )) and (citytable.dt = 'latest')");
    expectedClauses.add("c1_testdim4 testdim4 on testdim3.testdim4id = testdim4.id and ((( testdim4  .  name ) =  'TESTDIM4NAME' )) and (testdim4.dt = 'latest')");
    expectedClauses.add("c1_testdim3 testdim3 on testdim2.testdim3id = testdim3.id and (testdim3.dt = 'latest')");
    expectedClauses.add("c1_testdim2 testdim2 on testcube.dim2 = testdim2.id and (testdim2.dt = 'latest')");

    Set<String> actualClauses = new HashSet<String>();
    for (String clause : StringUtils.splitByWholeSeparator(rewrittenQuery.getAutoResolvedJoinChain(), "join")) {
      if (StringUtils.isNotBlank(clause))  {
        actualClauses.add(clause.trim());
      }
    }
    System.out.println("testPartialJoinResolverExpected" + expectedClauses);
    System.out.println("testPartialJoinResolverActual" + actualClauses);
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
    assertEquals("join c1_citytable citytable on testcube.cityid = citytable.id and (citytable.dt = 'latest')", joinClause.trim());
  }

  @Test
  public void testJoinTypeConf() throws Exception {
    hconf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "LEFTOUTER");
    System.out.println("@@Set join type to " + hconf.get(CubeQueryConfUtil.JOIN_TYPE_KEY));
    driver = new CubeQueryRewriter(hconf);
    String query = "select citytable.name, msr2 FROM testCube WHERE " + twoDaysRange;
    CubeQueryContext ctx = driver.rewrite(query);
    String hql = ctx.toHQL();
    System.out.println("testJoinTypeConf@@Resolved join clause1 - " + ctx.getAutoResolvedJoinChain());
    assertEquals("left outer join c1_citytable citytable on testcube.cityid = citytable.id and (citytable.dt = 'latest')",
      ctx.getAutoResolvedJoinChain().trim());

    hconf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "FULLOUTER");
    System.out.println("@@Set join type to " + hconf.get(CubeQueryConfUtil.JOIN_TYPE_KEY));
    driver = new CubeQueryRewriter(hconf);
    ctx = driver.rewrite(query);
    hql = ctx.toHQL();
    System.out.println("testJoinTypeConf@@Resolved join clause2 - "+ ctx.getAutoResolvedJoinChain());
    assertEquals("full outer join c1_citytable citytable on testcube.cityid = citytable.id and (citytable.dt = 'latest')",
      ctx.getAutoResolvedJoinChain().trim());
  }

  @Test
  public void testPreserveTableAlias() throws Exception {
    hconf.set(CubeQueryConfUtil.JOIN_TYPE_KEY, "LEFTOUTER");
    String query = "select c.name, t.msr2 FROM testCube t join citytable c WHERE " + twoDaysRange;
    CubeQueryRewriter driver = new CubeQueryRewriter(hconf);
    CubeQueryContext ctx = driver.rewrite(query);
    String hql = ctx.toHQL();
    System.out.println("testPreserveTableAlias@@HQL:" + hql);
    System.out.println("testPreserveTableAlias@@Resolved join clause - " + ctx.getAutoResolvedJoinChain());
    // Check that aliases are preserved in the join clause
    assertEquals("left outer join c1_citytable c on t.cityid = c.id and (c.dt = 'latest')", ctx.getAutoResolvedJoinChain().trim());
    String whereClause = hql.substring(hql.indexOf("WHERE"));
    // Check that the partition condition is not added again in where clause
    assertFalse(whereClause.contains("c.dt = 'latest'"));
  }

  @Test
  public void testDimOnlyQuery() throws Exception {
    String query = "select citytable.name, statetable.name from citytable limit 10";
    HiveConf dimOnlyConf = new HiveConf(hconf);
    //dimOnlyConf.set(CubeQueryConfUtil.DISABLE_AUTO_JOINS, "true");
    CubeQueryRewriter rewriter = new CubeQueryRewriter(dimOnlyConf);
    CubeQueryContext ctx = rewriter.rewrite(query);
    String hql = ctx.toHQL();
    assertTrue(hql.contains("WHERE (citytable.dt = 'latest') LIMIT 10"));
    System.out.println("testDimOnlyQuery@@@HQL:" + hql);
    System.out.println("testDimOnlyQuery@@@Resolved join clause: " + ctx.getAutoResolvedJoinChain());
    assertEquals("left outer join c1_statetable statetable on citytable.stateid = statetable.id and (statetable.dt = 'latest')",
        ctx.getAutoResolvedJoinChain().trim());

    String queryWithJoin = "select citytable.name, statetable.name from citytable join statetable";
    ctx = rewriter.rewrite(queryWithJoin);
    hql = ctx.toHQL();
    System.out.println("testDimOnlyQuery@@@HQL2:" + hql);
    HQLParser.parseHQL(hql);
  }
}
