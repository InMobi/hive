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


import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.*;

public class SchemaGraph {
  /*
   * An edge in the schema graph
   */
  public static class TableRelationship {
    final String fromColumn;
    final AbstractCubeTable fromTable;
    final String toColumn;
    final AbstractCubeTable toTable;

    public TableRelationship(String fromCol, AbstractCubeTable fromTab,
                             String toCol, AbstractCubeTable toTab) {
      fromColumn = fromCol;
      fromTable = fromTab;
      toColumn = toCol;
      toTable = toTab;
    }

    public String getFromColumn() {
      return fromColumn;
    }

    public String getToColumn() {
      return toColumn;
    }

    public AbstractCubeTable getFromTable() {
      return fromTable;
    }

    public AbstractCubeTable getToTable() {
      return toTable;
    }

    @Override
    public String toString() {
      return fromTable.getName() + "." + fromColumn + "->" + toTable.getName() + "." + toColumn;
    }

    @Override
    public boolean equals(Object obj) {
      //TODO Make sure that edges a->b and b->a are equal
      if (!(obj instanceof TableRelationship)) {
        return false;
      }

      TableRelationship other = (TableRelationship)obj;

      return fromColumn.equals(other.fromColumn) &&
        toColumn.equals(other.toColumn) &&
        fromTable.equals(other.fromTable) &&
        toTable.equals(other.toTable);
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }
  }

/**
 * Graph of tables in the cube metastore. Links between the tables are relationships in the cube.
 */
  private final CubeMetastoreClient metastore;
  // Graph for each cube
  private Map<CubeInterface, Map<AbstractCubeTable, Set<TableRelationship>>> cubeToGraph;

  // sub graph that contains only dimensions, mainly used while checking connectivity
  // between a set of dimensions
  private Map<AbstractCubeTable, Set<TableRelationship>> dimOnlySubGraph;

  public SchemaGraph(CubeMetastoreClient metastore) {
    this.metastore = metastore;
  }

  public Map<AbstractCubeTable, Set<TableRelationship>> getCubeGraph(CubeInterface cube) {
    return cubeToGraph.get(cube);
  }

  public Map<AbstractCubeTable, Set<TableRelationship>> getDimOnlyGraph() {
    return dimOnlySubGraph;
  }

  /**
   * Build the schema graph for all cubes and dimensions
   * @return
   * @throws org.apache.hadoop.hive.ql.metadata.HiveException
   */
  public void buildSchemaGraph() throws HiveException {
    cubeToGraph = new HashMap<CubeInterface, Map<AbstractCubeTable, Set<TableRelationship>>>();
    for (CubeInterface cube : metastore.getAllCubes()) {
      Map<AbstractCubeTable, Set<TableRelationship>> graph =
        new HashMap<AbstractCubeTable, Set<TableRelationship>>();
      buildGraph((AbstractCubeTable)cube, graph);

      for (UberDimension dim : metastore.getAllUberDimensions()) {
        buildGraph(dim, graph);
      }

      cubeToGraph.put(cube, graph);
    }

    dimOnlySubGraph = new HashMap<AbstractCubeTable, Set<TableRelationship>>();
    for (UberDimension dim : metastore.getAllUberDimensions()) {
      buildGraph(dim, dimOnlySubGraph);
    }
  }

  private List<CubeDimension> getRefDimensions(AbstractCubeTable cube) throws HiveException {
    List<CubeDimension> refDimensions = new ArrayList<CubeDimension>();
    Set<CubeDimension> allAttrs = null;
    if (cube instanceof CubeInterface) {
      allAttrs = ((CubeInterface)cube).getDimensions();
    } else if (cube instanceof UberDimension) {
      allAttrs = ((UberDimension)cube).getAttributes();
    } else {
      throw new HiveException("Not a valid table type" + cube);
    }
    // find out all dimensions which link to other dimension tables
    for (CubeDimension dim : allAttrs) {
      if (dim instanceof ReferencedDimension) {
        refDimensions.add(dim);
      } else if (dim instanceof HierarchicalDimension) {
        for (CubeDimension hdim : ((HierarchicalDimension)dim).getHierarchy()) {
          if (hdim instanceof ReferencedDimension) {
            refDimensions.add(hdim);
          }
        }
      }
    }
    return refDimensions;
  }
  // Build schema graph for a cube
  private void buildGraph(AbstractCubeTable cubeTable, Map<AbstractCubeTable, Set<TableRelationship>> graph)
    throws HiveException {
    List<CubeDimension> refDimensions = getRefDimensions(cubeTable);

    // build graph for each linked dimension
    for (CubeDimension dim : refDimensions) {
      // Find out references leading from dimension columns of the cube if any
      if (dim instanceof ReferencedDimension) {
        ReferencedDimension refDim = (ReferencedDimension) dim;
        List<TableReference> refs = refDim.getReferences();

        for (TableReference ref : refs) {
          String destColumnName = ref.getDestColumn();
          String destTableName = ref.getDestTable();

          if (metastore.isUberDimension(destTableName)) {
            // Cube -> Dimension reference
            UberDimension relatedDim = metastore.getUberDimension(destTableName);
            addLinks(refDim.getName(), cubeTable, destColumnName, relatedDim, graph);
          } else {
            throw new HiveException("Dim -> Cube references are not supported: "
              + dim.getName() + "." + refDim.getName() + "->" + destTableName + "." + destColumnName);
          }
        } // end loop for refs from a dim
      }
    }
  }

  private void addLinks(String col1, AbstractCubeTable tab1, String col2, AbstractCubeTable tab2,
                        Map<AbstractCubeTable, Set<TableRelationship>> graph) {

    TableRelationship rel1 = new TableRelationship(col1, tab1, col2, tab2);
    TableRelationship rel2 = new TableRelationship(col2, tab2, col1, tab1);

    Set<TableRelationship> inEdges = graph.get(tab2);
    if (inEdges == null) {
      inEdges = new LinkedHashSet<TableRelationship>();
      graph.put(tab2, inEdges);
    }
    inEdges.add(rel1);

    Set<TableRelationship> outEdges = graph.get(tab1);
    if (outEdges == null) {
      outEdges = new LinkedHashSet<TableRelationship>();
      graph.put(tab1, outEdges);
    }

    outEdges.add(rel2);

  }


  // This returns the first path found between the dimTable and the target
  private boolean findJoinChain(UberDimension dimTable, AbstractCubeTable target,
                                Map<AbstractCubeTable, Set<TableRelationship>> graph,
                                List<TableRelationship> chain,
                                Set<AbstractCubeTable> visited)
  {
    // Mark node as visited
    visited.add(dimTable);

    Set<TableRelationship> edges = graph.get(dimTable);
    if (edges == null || edges.isEmpty()) {
      return false;
    }
    boolean foundPath = false;
    for (TableRelationship edge : edges) {
      if (visited.contains(edge.fromTable)) {
        continue;
      }

      if (edge.fromTable.equals(target)) {
        chain.add(edge);
        // Search successful
        foundPath = true;
        break;
      } else if (edge.fromTable instanceof UberDimension) {
        List<TableRelationship> tmpChain = new ArrayList<TableRelationship>();
        if (findJoinChain((UberDimension) edge.fromTable, target,
          graph, tmpChain, visited)) {
          // This dim eventually leads to the cube
          chain.add(edge);
          chain.addAll(tmpChain);
          foundPath = true;
          break;
        }
      } // else - this edge doesn't lead to the target, try next one
    }

    return foundPath;
  }

  /**
   * Find if there is a join chain (path) between the given dimension table and the target table
   * @param joinee
   * @param target
   * @param chain
   * @return
   */
  public boolean findJoinChain(UberDimension joinee, AbstractCubeTable target,
                               List<TableRelationship> chain) {
    if (target instanceof CubeInterface) {
      return findJoinChain(joinee, target, cubeToGraph.get(target), chain,
        new HashSet<AbstractCubeTable>());
    } else if (target instanceof UberDimension) {
      return findJoinChain(joinee, target, dimOnlySubGraph, chain,
        new HashSet<AbstractCubeTable>());
    } else {
      return false;
    }
  }

  public void print() {
    for (CubeInterface cube : cubeToGraph.keySet()) {
      Map<AbstractCubeTable, Set<TableRelationship>> graph = cubeToGraph.get(cube);
      System.out.println("**Cube " + cube.getName());
      System.out.println("--Graph-Nodes=" + graph.size());
      for (AbstractCubeTable tab : graph.keySet()) {
        System.out.println(tab.getName() + "::" + graph.get(tab));
      }
    }
    System.out.println("**Dim only subgraph");
    System.out.println("--Graph-Nodes=" + dimOnlySubGraph.size());
    for (AbstractCubeTable tab : dimOnlySubGraph.keySet()) {
      System.out.println(tab.getName() + "::" + dimOnlySubGraph.get(tab));
    }
  }
}
