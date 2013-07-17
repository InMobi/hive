package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.AbstractCubeTable;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.parse.JoinResolver;
import org.apache.hadoop.hive.ql.cube.parse.JoinResolver.TableRelationship;
import org.apache.hadoop.hive.ql.cube.processors.TestCubeDriver;
import org.junit.AfterClass;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJoinResolver {
  
  private  static CubeTestSetup setup;
  private  static HiveConf hconf = new HiveConf(TestJoinResolver.class);;


  @BeforeClass
  public static void setup() throws Exception {
    setup = new CubeTestSetup();
    setup.createSources(hconf, TestCubeDriver.class.getSimpleName());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    //setup.dropSources(hconf);
  }

  private CubeMetastoreClient metastore;

  
  @Before
  public void setupInstance() throws Exception {
    this.metastore = CubeMetastoreClient.getInstance(hconf);
  }
  
  
  // testBuildGraph - graph correctness
  @Test
  public void testBuildGraph() throws Exception {
    JoinResolver resolver = new JoinResolver(hconf);
    
    Map<AbstractCubeTable, Set<TableRelationship>> graph = resolver.buildSchemaGraph();
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
    JoinResolver resolver = new JoinResolver(hconf);
    Cube cube = metastore.getCube(CubeTestSetup.TEST_CUBE_NAME);
    Map<AbstractCubeTable, Set<TableRelationship>> graph = resolver.buildSchemaGraph();
    assertNotNull(graph);
    
    CubeDimensionTable testDim4 = metastore.getDimensionTable("testdim4");
    List<TableRelationship> chain = new ArrayList<TableRelationship>();
    assertTrue(resolver.findJoinChain(testDim4, cube, graph, chain));
    System.out.println(chain);
    
    chain.clear();
    CubeDimensionTable cityTable = metastore.getDimensionTable("citytable");
    assertTrue(resolver.findJoinChain(cityTable, cube, graph, chain));
    System.out.println("City -> cube chain: " + chain);
    // find chains between dimensions
    chain.clear();
    CubeDimensionTable stateTable = metastore.getDimensionTable("statetable");
    assertTrue(resolver.findJoinChain(stateTable, cityTable, graph, chain));
    System.out.println("Dim chain state -> city : " + chain);
    
    chain.clear();
    CubeDimensionTable dim2 = metastore.getDimensionTable("testdim2");
    assertTrue(resolver.findJoinChain(testDim4, dim2, graph, chain));
    System.out.println("Dim chain testdim4 -> testdim2 : " + chain);
  }
  
  private void printGraph(Map<AbstractCubeTable, Set<TableRelationship>> graph) {
    System.out.println("--Graph-Nodes=" + graph.size());
    for (AbstractCubeTable tab : graph.keySet()) {
      System.out.println(tab.getName() + "::" + graph.get(tab));
    }
  }
}
