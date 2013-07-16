package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

import org.junit.After;
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
    Cube cube = metastore.getCube(CubeTestSetup.TEST_CUBE_NAME);
    Map<AbstractCubeTable, List<TableRelationship>> graph = resolver.buildGraph(cube);
    assertNotNull(graph);
    
    for (AbstractCubeTable refDim : graph.keySet()) {
      System.out.println("edges for " + refDim.getName()+ " : " + graph.get(refDim));
    }
    
    // Let's do some lookups
    List<TableRelationship> dim4Edges = graph.get(metastore.getDimensionTable("testdim4"));
    assertNotNull(dim4Edges);
    assertEquals(1, dim4Edges.size());
    
    TableRelationship dim4edge = dim4Edges.get(0);
    assertEquals("id", dim4edge.toColumn);
    assertEquals(metastore.getDimensionTable("testdim4"), dim4edge.toTable);
    assertEquals("testdim4id", dim4edge.fromColumn);
    assertEquals(metastore.getDimensionTable("testdim3"), dim4edge.fromTable);
  }
  
  @Test
  public void testFindChain() throws Exception {
    JoinResolver resolver = new JoinResolver(hconf);
    Cube cube = metastore.getCube(CubeTestSetup.TEST_CUBE_NAME);
    Map<AbstractCubeTable, List<TableRelationship>> graph = resolver.buildGraph(cube);
    assertNotNull(graph);
    
    CubeDimensionTable dimTable = metastore.getDimensionTable("testdim4");
    List<TableRelationship> chain = new ArrayList<TableRelationship>();
    assertTrue(resolver.findJoinChain(dimTable, cube, graph, chain));
    System.out.println(chain);
    
    chain.clear();
    CubeDimensionTable cityTable = metastore.getDimensionTable("citytable");
    assertFalse(resolver.findJoinChain(cityTable, cube, graph, chain));
  }
}
