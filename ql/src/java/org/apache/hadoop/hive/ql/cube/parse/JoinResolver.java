package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.AbstractCubeTable;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimension;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.ReferencedDimension;
import org.apache.hadoop.hive.ql.cube.metadata.TableReference;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
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
  }

  public JoinResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    try {
      resolveJoins((CubeQueryContext) cubeql);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  public void resolveJoins(CubeQueryContext cubeql) throws SemanticException {
    QB cubeQB = cubeql.getQB();
    if (cubeQB.getParseInfo().getJoinExpr() != null) {
      cubeQB.setQbJoinTree(genJoinTree(cubeQB, cubeQB.getParseInfo().getJoinExpr(), cubeql));
    } else {
      // Try resolving joins automatically.
      autoResolveJoins(cubeql);
    }
  }

  void autoResolveJoins(CubeQueryContext cubeql) throws SemanticException {
    Cube cube = cubeql.getCube();
    
    if (cube == null) {
      LOG.warn("Can't auto resolve joins as cube is null");
      cubeql.setJoinsResolvedAutomatically(false);
      return;
    }
    LOG.info("Resolving joins automatically for cube " + cube.getName());
    
    Map<AbstractCubeTable, List<TableRelationship>> graph;
    try {
      graph = buildGraph(cube);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
    
    Set<CubeDimensionTable> dimTables = cubeql.getDimensionTables();
    Map<CubeDimensionTable, List<TableRelationship>> joinChain = 
        new LinkedHashMap<CubeDimensionTable, List<TableRelationship>>();

    // Resolve join path for each dimension accessed in the query
    for (CubeDimensionTable joinee : dimTables) {
      ArrayList<TableRelationship> chain = new ArrayList<TableRelationship>();
      if (findJoinChain(joinee, cube, graph, chain)) {
        joinChain.put(joinee, chain);
      } else {
        // No link to cube from this dim, can't proceed with query
        throw new SemanticException("No join path from dimension table " + joinee.getName() 
            + " to cube " + cube.getName());
      }
    }
    
    cubeql.setJoinsResolvedAutomatically(true);
    cubeql.setAutoResolvedJoinChain(joinChain);
  }
  
  Map<AbstractCubeTable, List<TableRelationship>> buildGraph(Cube cube) throws HiveException {
    // Vertex is a table
    // Edges are links between tables. An edge links a column of source table to a column
    // of dest table
    Set<CubeDimension> dimensions = cube.getDimensions();
    CubeMetastoreClient metastore = CubeMetastoreClient.getInstance(new HiveConf(this.getClass()));
    Map<AbstractCubeTable, List<TableRelationship>> schemaGraph = 
        new HashMap<AbstractCubeTable, List<TableRelationship>>();

    for (CubeDimension dim : dimensions) {
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
            
            TableRelationship rel = new TableRelationship(refDim.getName(), cube, 
                destColumnName, relatedDim);
            
            List<TableRelationship> edges = schemaGraph.get(relatedDim);
            
            if (edges == null) {
              edges = new ArrayList<TableRelationship>();
              schemaGraph.put(relatedDim, edges);
            }
            edges.add(rel);
           
            // build graph for the related dim
            buildDimGraph(metastore, relatedDim, schemaGraph);
           
          } else if (metastore.isFactTable(destTableName)) {
            throw new HiveException("Cube -> Fact references are not supported");
          }
        } // end loop for refs from a dim
      }
    }
    
    return schemaGraph;
  }
  
  
  void buildDimGraph(CubeMetastoreClient metastore, CubeDimensionTable tab, 
      Map<AbstractCubeTable, List<TableRelationship>> graph) throws HiveException {
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
            TableRelationship rel = new TableRelationship(colName, tab, destCol, relTab);
            List<TableRelationship> edges = graph.get(relTab);
            if (edges == null) {
              edges = new ArrayList<TableRelationship>();
              graph.put(relTab, edges);
            }
            edges.add(rel);
            // recurse down to build graph for the referenced dim
            buildDimGraph(metastore, relTab, graph);
          } else {
            // not dealing with dim->fact references
            throw new HiveException("Dimension -> Fact references not supported");
          }
        } // end loop for refs from a column
      }
    }
  }

  // Find if there is a join chain starting from the dimension to the cube
  boolean findJoinChain(CubeDimensionTable dimTable, Cube cube, 
      Map<AbstractCubeTable, List<TableRelationship>> graph, List<TableRelationship> chain) {
    
    List<TableRelationship> edges = graph.get(dimTable);
    if (edges == null || edges.isEmpty()) {
      return false;
    }
    boolean foundPath = false;
    for (TableRelationship edge : edges) {
      if (edge.fromTable == cube) {
        chain.add(edge);
        // Search successful
        foundPath = true;
        break;
      } else if (edge.fromTable instanceof CubeDimensionTable) {
        List<TableRelationship> tmpChain = new ArrayList<TableRelationship>();
        if (findJoinChain((CubeDimensionTable)edge.fromTable, cube, graph, tmpChain)) {
          // This dim eventually leads to the cube
          chain.add(edge);
          chain.addAll(tmpChain);
          foundPath = true;
          break;
        }
      } // else - this edge doesn't lead to the cube, try next one
    }
    
    return foundPath;
  }

  // Recursively find out join conditions
  private QBJoinTree genJoinTree(QB qb, ASTNode joinParseTree,
      CubeQueryContext cubeql)
      throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    JoinCond[] condn = new JoinCond[1];

    // Figure out join condition descriptor
    switch (joinParseTree.getToken().getType()) {
    case HiveParser.TOK_LEFTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTOUTER);
      break;
    case HiveParser.TOK_RIGHTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.RIGHTOUTER);
      break;
    case HiveParser.TOK_FULLOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.FULLOUTER);
      break;
    case HiveParser.TOK_LEFTSEMIJOIN:
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
    if ((left.getToken().getType() == HiveParser.TOK_TABREF)
        || (left.getToken().getType() == HiveParser.TOK_SUBQUERY)) {
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

    if ((right.getToken().getType() == HiveParser.TOK_TABREF)
        || (right.getToken().getType() == HiveParser.TOK_SUBQUERY)) {
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
    if ((node.getToken().getType() == HiveParser.TOK_JOIN)
        || (node.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN)
        || (node.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN)
        || (node.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN)
        || (node.getToken().getType() == HiveParser.TOK_LEFTSEMIJOIN)
        || (node.getToken().getType() == HiveParser.TOK_UNIQUEJOIN)) {
      return true;
    }
    return false;
  }
}
