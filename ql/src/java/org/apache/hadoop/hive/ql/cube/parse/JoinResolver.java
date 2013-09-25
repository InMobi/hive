package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.AbstractCubeTable;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimension;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.HierarchicalDimension;
import org.apache.hadoop.hive.ql.cube.metadata.ReferencedDimension;
import org.apache.hadoop.hive.ql.cube.metadata.TableReference;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.JoinCond;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
/**
 * 
 * JoinResolver.
 *
 */
public class JoinResolver implements ContextRewriter {

  private static final Log LOG = LogFactory.getLog(JoinResolver.class);
  public static final String DISABLE_AUTO_JOINS = "hive.cube.disable.auto.join";
  public static final String JOIN_TYPE_KEY = "hive.cube.join.type";

  public static class AutoJoinContext {
    private final Map<CubeDimensionTable, List<TableRelationship>> joinChain;
    private final Map<AbstractCubeTable, String> partialJoinConditions;

    public AutoJoinContext(Map<CubeDimensionTable, List<TableRelationship>> joinChain,
                            Map<AbstractCubeTable, String> partialJoinConditions) {
      this.joinChain = joinChain;
      this.partialJoinConditions = partialJoinConditions;
    }

    public Map<CubeDimensionTable, List<TableRelationship>> getJoinChain() {
      return joinChain;
    }

    public Map<AbstractCubeTable, String> getPartialJoinConditions() {
      return partialJoinConditions;
    }

    public String getMergedJoinClause(Configuration conf,
                                      Map<String, String> dimStorageTableToWhereClause,
                                      Map<AbstractCubeTable, Set<String>> storageTableToQuery) {
      for (List<TableRelationship> chain : joinChain.values()) {
        // Need to reverse the chain so that left most table in join comes first
        Collections.reverse(chain);
      }

      Set<String> clauses = new LinkedHashSet<String>();

      String joinTypeCfg = conf.get(JOIN_TYPE_KEY);
      String joinType = "";

      if (StringUtils.isNotBlank(joinTypeCfg)) {
        JoinType type = JoinType.valueOf(joinTypeCfg.toUpperCase());
        switch (type) {
          case FULLOUTER:
            joinType = "full outer";
            break;
          case INNER:
            joinType = "inner";
            break;
          case LEFTOUTER:
            joinType = "left outer";
            break;
          case LEFTSEMI:
            joinType = "left semi";
            break;
          case UNIQUE:
            joinType = "unique";
            break;
          case RIGHTOUTER:
            joinType = "right outer";
            break;
        }
      }

      for (List<TableRelationship> chain : joinChain.values()) {
        for (TableRelationship rel : chain) {
          StringBuilder clause = new StringBuilder(joinType)
            .append(" join ")
            .append(rel.toTable.getName())
            .append(" on ")
            .append(rel.fromTable.getName()).append(".").append(rel.fromColumn)
            .append(" = ").append(rel.toTable.getName()).append(".").append(rel.toColumn);
          // Check if user specified a join clause, if yes, add it
          String filter = partialJoinConditions.get(rel.toTable);

          if (StringUtils.isNotBlank(filter)) {
            clause.append(" and (").append(filter).append(")");
          }

          String whereClause = null;
          if (dimStorageTableToWhereClause != null && storageTableToQuery != null) {
            Set<String> queries = storageTableToQuery.get(rel.toTable);
            if (queries != null) {
              String storageTableKey = queries.iterator().next();
              if (StringUtils.isNotBlank(storageTableKey)) {
                whereClause = dimStorageTableToWhereClause.get(queries.iterator().next());
              }
            }
          }

          if (StringUtils.isNotBlank(whereClause)) {
            clause.append (" and (").append(whereClause).append(")");
          }
          clauses.add(clause.toString());
        }
      }

      return StringUtils.join(clauses, " ");
    }

  }
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
    
    public String getJoinCondition() {
      return fromTable.getName() + "." + fromColumn + "=" + toTable.getName() + "." + toColumn;
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
  public static class SchemaGraph {
    private final CubeMetastoreClient metastore;
    // Graph for each cube
    private Map<Cube, Map<AbstractCubeTable, Set<TableRelationship>>> cubeToGraph;

    // sub graph that contains only dimensions, mainly used while checking connectivity
    // between a set of dimensions
    private Map<AbstractCubeTable, Set<TableRelationship>> dimOnlySubGraph;

    public SchemaGraph(CubeMetastoreClient metastore) {
      this.metastore = metastore;
    }

    protected Map<AbstractCubeTable, Set<TableRelationship>> getCubeGraph(Cube cube) {
      return cubeToGraph.get(cube);
    }

    protected Map<AbstractCubeTable, Set<TableRelationship>> getDimOnlyGraph() {
      return dimOnlySubGraph;
    }

    /**
     * Build the schema graph for all cubes and dimensions
     * @return
     * @throws HiveException
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

  private CubeMetastoreClient metastore;
  private Map<AbstractCubeTable, String> partialJoinConditions;
  private AbstractCubeTable target;
  private HiveConf conf;

  public JoinResolver(Configuration conf) {
    partialJoinConditions = new HashMap<AbstractCubeTable, String>();
  }
  
  private CubeMetastoreClient getMetastoreClient() throws HiveException {
    if (metastore == null) {
      metastore = CubeMetastoreClient.getInstance(new HiveConf(this.getClass()));
    }
    
    return metastore;
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    try {
      conf = cubeql.getHiveConf();
      resolveJoins(cubeql);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  public void resolveJoins(CubeQueryContext cubeql) throws HiveException {
    QB cubeQB = cubeql.getQB();
    boolean joinResolverDisabled = conf.getBoolean(DISABLE_AUTO_JOINS, false);
    if (joinResolverDisabled) {
      if (cubeQB.getParseInfo().getJoinExpr() != null) {
        cubeQB.setQbJoinTree(genJoinTree(cubeQB, cubeQB.getParseInfo().getJoinExpr(), cubeql));
      }
    } else {
      autoResolveJoins(cubeql);
    }
  }

  protected SchemaGraph getSchemaGraph() throws HiveException {
    SchemaGraph graph = getMetastoreClient().getSchemaGraph();
    if (graph == null) {
      graph = new SchemaGraph(getMetastoreClient());
      graph.buildSchemaGraph();
      getMetastoreClient().setSchemaGraph(graph);
    }
    return graph;
  }
  /**
   * Resolve joins automatically for the given query.
   * @param cubeql
   * @throws SemanticException
   */
  public void autoResolveJoins(CubeQueryContext cubeql) throws HiveException {
    // Check if this query needs a join -
    // A join is needed if there is a cube and at least one dimension, or, 0 cubes and more than one
    // dimensions
    
    Set<CubeDimensionTable> autoJoinDims = cubeql.getAutoJoinDimensions();
    // Add dimensions specified in the partial join tree
    ASTNode joinClause = cubeql.getQB().getParseInfo().getJoinExpr();
    if (joinClause == null) {
      // Only cube in the query
      if (cubeql.hasCubeInQuery()) {
        target = cubeql.getCube();
      } else {
        String targetDimAlias = cubeql.getQB().getTabAliases().iterator().next();
        String targetDimTable = cubeql.getQB().getTabNameForAlias(targetDimAlias);
        if (targetDimTable == null) {
          LOG.warn("Null table for alias " + targetDimAlias);
        }
        target = getMetastoreClient().getDimensionTable(targetDimTable);
      }
    }
    searchDimensionTables(joinClause);
    if (target == null) {
      LOG.warn("Can't resolve joins for null target");
      return;
    }
    boolean hasDimensions = (autoJoinDims != null && !autoJoinDims.isEmpty()) || !partialJoinConditions.isEmpty();
    // Query has a cube and at least one dimension
    boolean cubeAndDimQuery = cubeql.hasCubeInQuery() && hasDimensions;
    // This query has only dimensions in it
    boolean dimOnlyQuery = !cubeql.hasCubeInQuery() && hasDimensions;
        
    if (!cubeAndDimQuery && !dimOnlyQuery) {
      return;
    }
    
    Set<CubeDimensionTable> dimTables =
        new HashSet<CubeDimensionTable>(autoJoinDims);
    for (AbstractCubeTable partiallyJoinedTable : partialJoinConditions.keySet()) {
      dimTables.add((CubeDimensionTable) partiallyJoinedTable);
    }
    // Remove target
    dimTables.remove(target);
    if (dimTables.isEmpty()) {
      // Joins not required
      return;
    }

    SchemaGraph graph = getSchemaGraph();
    Map<CubeDimensionTable, List<TableRelationship>> joinChain =
        new LinkedHashMap<CubeDimensionTable, List<TableRelationship>>();
    // Resolve join path for each dimension accessed in the query
    boolean joinsResolved = false;
    for (CubeDimensionTable joinee : dimTables) {
      ArrayList<TableRelationship> chain = new ArrayList<TableRelationship>();
      if (graph.findJoinChain(joinee, target, chain)) {
        joinsResolved = true;
        joinChain.put(joinee, chain);
      } else {
        // No link to cube from this dim, can't proceed with query
        throw new SemanticException("No join path from " + joinee.getName() 
            + " to " + target.getName());
      }
    }
    
    if (joinsResolved) {
      for (List<TableRelationship> chain : joinChain.values()) {
        for (TableRelationship rel : chain) {
          if (rel.toTable instanceof CubeDimensionTable) {
            autoJoinDims.add((CubeDimensionTable) rel.toTable);
          }
          if (rel.fromTable instanceof  CubeDimensionTable) {
            autoJoinDims.add((CubeDimensionTable) rel.fromTable);
          }
        }
      }
      AutoJoinContext joinCtx = new AutoJoinContext(joinChain, partialJoinConditions);
      cubeql.setAutoJoinCtx(joinCtx);
      cubeql.setJoinsResolvedAutomatically(joinsResolved);
    }
  }

  private void setTarget(ASTNode node) throws HiveException {
    String targetTableName =
      HQLParser.getString(HQLParser.findNodeByPath(node, TOK_TABNAME, Identifier));
    if (getMetastoreClient().isDimensionTable(targetTableName)) {
      target = getMetastoreClient().getDimensionTable(targetTableName);
    } else if (getMetastoreClient().isCube(targetTableName)) {
      target = getMetastoreClient().getCube(targetTableName);
    } else {
      throw new SemanticException("Target table is neither dimension nor cube: " + targetTableName);
    }
  }

  private void searchDimensionTables(ASTNode node) throws HiveException {
    if (node == null) {
      return;
    }

    if (isJoinToken(node)) {
      ASTNode left = (ASTNode) node.getChild(0);
      ASTNode right = (ASTNode) node.getChild(1);
      // Get table name and

      String tableName = HQLParser.getString(HQLParser.findNodeByPath(right,
        TOK_TABNAME, Identifier));

      CubeDimensionTable dimensionTable = getMetastoreClient().getDimensionTable(tableName);
      String joinCond = "";
      if (node.getChildCount() > 2) {
        // User has specified a join condition for filter pushdown.
        joinCond = HQLParser.getString((ASTNode) node.getChild(2));
      }
      partialJoinConditions.put(dimensionTable, joinCond);

      if (isJoinToken(left)) {
        searchDimensionTables(left);
      } else {
        if (left.getToken().getType() == TOK_TABREF) {
          setTarget(left);
        }
      }
    } else if (node.getToken().getType() == TOK_TABREF) {
       setTarget(node);
    }

  }

  // Recursively find out join conditions
  private QBJoinTree genJoinTree(QB qb, ASTNode joinParseTree,
      CubeQueryContext cubeql)
      throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    JoinCond[] condn = new JoinCond[1];

    // Figure out join condition descriptor
    switch (joinParseTree.getToken().getType()) {
    case TOK_LEFTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTOUTER);
      break;
    case TOK_RIGHTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.RIGHTOUTER);
      break;
    case TOK_FULLOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.FULLOUTER);
      break;
    case TOK_LEFTSEMIJOIN:
      joinTree.setNoSemiJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTSEMI);
      break;
    default:
      condn[0] = new JoinCond(0, 1, JoinType.INNER);
      joinTree.setNoOuterJoin(true);
      break;
    }

    joinTree.setJoinCond(condn);

    ASTNode left = (ASTNode) joinParseTree.getChild(0);
    ASTNode right = (ASTNode) joinParseTree.getChild(1);

    // Left subtree is table or a subquery
    if ((left.getToken().getType() == TOK_TABREF)
        || (left.getToken().getType() == TOK_SUBQUERY)) {
      String tableName = SemanticAnalyzer.getUnescapedUnqualifiedTableName(
          (ASTNode) left.getChild(0)).toLowerCase();
      String alias = left.getChildCount() == 1 ? tableName
          : SemanticAnalyzer.unescapeIdentifier(left.getChild(left.getChildCount() - 1)
                  .getText().toLowerCase());

      joinTree.setLeftAlias(alias);

      String[] leftAliases = new String[1];
      leftAliases[0] = alias;
      joinTree.setLeftAliases(leftAliases);

      String[] children = new String[2];
      children[0] = alias;
      joinTree.setBaseSrc(children);

    } else if (isJoinToken(left)) {
      // Left subtree is join token itself, so recurse down
      QBJoinTree leftTree = genJoinTree(qb, left, cubeql);

      joinTree.setJoinSrc(leftTree);

      String[] leftChildAliases = leftTree.getLeftAliases();
      String leftAliases[] = new String[leftChildAliases.length + 1];
      for (int i = 0; i < leftChildAliases.length; i++) {
        leftAliases[i] = leftChildAliases[i];
      }
      leftAliases[leftChildAliases.length] = leftTree.getRightAliases()[0];
      joinTree.setLeftAliases(leftAliases);

    } else {
      assert (false);
    }

    if ((right.getToken().getType() == TOK_TABREF)
        || (right.getToken().getType() == TOK_SUBQUERY)) {
      String tableName = SemanticAnalyzer.getUnescapedUnqualifiedTableName(
          (ASTNode) right.getChild(0)).toLowerCase();
      String alias = right.getChildCount() == 1 ? tableName
          : SemanticAnalyzer.unescapeIdentifier(right.getChild(
              right.getChildCount() - 1).getText().toLowerCase());
      String[] rightAliases = new String[1];
      rightAliases[0] = alias;
      joinTree.setRightAliases(rightAliases);
      String[] children = joinTree.getBaseSrc();
      if (children == null) {
        children = new String[2];
      }
      children[1] = alias;
      joinTree.setBaseSrc(children);
      // remember rhs table for semijoin
      if (joinTree.getNoSemiJoin() == false) {
        joinTree.addRHSSemijoin(alias);
      }
    } else {
      assert false;
    }

    ASTNode joinCond = (ASTNode) joinParseTree.getChild(2);
    if (joinCond != null) {
      cubeql.setJoinCond(joinTree, HQLParser.getString(joinCond));
    } else {
      // No join condition specified. this should be an error
      new SemanticException("Join condition not specified");
    }
    return joinTree;
  }

  private boolean isJoinToken(ASTNode node) {
    if ((node.getToken().getType() == TOK_JOIN)
        || (node.getToken().getType() == TOK_LEFTOUTERJOIN)
        || (node.getToken().getType() == TOK_RIGHTOUTERJOIN)
        || (node.getToken().getType() == TOK_FULLOUTERJOIN)
        || (node.getToken().getType() == TOK_LEFTSEMIJOIN)
        || (node.getToken().getType() == TOK_UNIQUEJOIN)) {
      return true;
    }
    return false;
  }
}
