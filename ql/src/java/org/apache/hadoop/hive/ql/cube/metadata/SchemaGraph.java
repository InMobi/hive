package org.apache.hadoop.hive.ql.cube.metadata;

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
  private Map<Cube, Map<AbstractCubeTable, Set<TableRelationship>>> cubeToGraph;

  // sub graph that contains only dimensions, mainly used while checking connectivity
  // between a set of dimensions
  private Map<AbstractCubeTable, Set<TableRelationship>> dimOnlySubGraph;

  public SchemaGraph(CubeMetastoreClient metastore) {
    this.metastore = metastore;
  }

  public Map<AbstractCubeTable, Set<TableRelationship>> getCubeGraph(Cube cube) {
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
    cubeToGraph = new HashMap<Cube, Map<AbstractCubeTable, Set<TableRelationship>>>();
    for (Cube cube : metastore.getAllCubes()) {
      Map<AbstractCubeTable, Set<TableRelationship>> graph =
        new HashMap<AbstractCubeTable, Set<TableRelationship>>();
      buildCubeGraph(cube, graph);

      for (CubeDimensionTable dim : metastore.getAllDimensionTables()) {
        buildDimGraph(dim, graph);
      }

      cubeToGraph.put(cube, graph);
    }

    dimOnlySubGraph = new HashMap<AbstractCubeTable, Set<TableRelationship>>();
    for (CubeDimensionTable dim : metastore.getAllDimensionTables()) {
      buildDimGraph(dim, dimOnlySubGraph);
    }
  }

  // Build schema graph for a cube
  private void buildCubeGraph(Cube cube, Map<AbstractCubeTable, Set<TableRelationship>> graph)
    throws HiveException {
    List<CubeDimension> refDimensions = new ArrayList<CubeDimension>();
    // find out all dimensions which link to other dimension tables
    for (CubeDimension dim : cube.getDimensions()) {
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

    // build graph for each linked dimension
    for (CubeDimension dim : refDimensions) {
      // Find out references leading from dimension columns of the cube if any
      if (dim instanceof ReferencedDimension) {
        ReferencedDimension refDim = (ReferencedDimension) dim;
        List<TableReference> refs = refDim.getReferences();

        for (TableReference ref : refs) {
          String destColumnName = ref.getDestColumn();
          String destTableName = ref.getDestTable();

          if (metastore.isDimensionTable(destTableName)) {
            // Cube -> Dimension reference
            CubeDimensionTable relatedDim = metastore.getDimensionTable(destTableName);
            addLinks(refDim.getName(), cube, destColumnName, relatedDim, graph);
          } else if (metastore.isFactTable(destTableName)) {
            throw new HiveException("Dim -> Fact references are not supported: "
              + dim.getName() + "." + refDim.getName() + "->" + destTableName + "." + destColumnName);
          }
        } // end loop for refs from a dim
      }
    }
  }

  // Build schema graph starting at a dimension
  private void buildDimGraph(CubeDimensionTable tab,
                             Map<AbstractCubeTable, Set<TableRelationship>> graph) throws HiveException {
    Map<String, List<TableReference>> references = tab.getDimensionReferences();

    if (references != null && !references.isEmpty()) {
      // for each column that leads to another dim table
      for (Map.Entry<String, List<TableReference>> referredColumn : references.entrySet()) {
        String colName = referredColumn.getKey();
        List<TableReference> dests = referredColumn.getValue();

        // for each link which leads from this column
        for (TableReference destRef : dests) {
          String destCol = destRef.getDestColumn();
          String destTab = destRef.getDestTable();

          if (metastore.isDimensionTable(destTab)) {
            CubeDimensionTable relTab = metastore.getDimensionTable(destTab);
            addLinks(colName, tab, destCol, relTab, graph);
          } else {
            // not dealing with dim->fact references
            throw new HiveException("Dimension -> Fact references not supported");
          }
        } // end loop for refs from a column
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
  private boolean findJoinChain(CubeDimensionTable dimTable, AbstractCubeTable target,
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
      } else if (edge.fromTable instanceof CubeDimensionTable) {
        List<TableRelationship> tmpChain = new ArrayList<TableRelationship>();
        if (findJoinChain((CubeDimensionTable) edge.fromTable, target,
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
  public boolean findJoinChain(CubeDimensionTable joinee, AbstractCubeTable target,
                               List<TableRelationship> chain) {
    if (target instanceof Cube) {
      return findJoinChain(joinee, target, cubeToGraph.get(target), chain,
        new HashSet<AbstractCubeTable>());
    } else if (target instanceof CubeDimensionTable) {
      return findJoinChain(joinee, target, dimOnlySubGraph, chain,
        new HashSet<AbstractCubeTable>());
    } else {
      return false;
    }
  }
}
