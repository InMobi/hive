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


import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FULLOUTERJOIN;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_JOIN;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_LEFTOUTERJOIN;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_LEFTSEMIJOIN;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_RIGHTOUTERJOIN;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_SUBQUERY;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABNAME;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABREF;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_UNIQUEJOIN;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.cube.metadata.AbstractCubeTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.SchemaGraph;
import org.apache.hadoop.hive.ql.cube.metadata.SchemaGraph.TableRelationship;
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

  /**
   * Store join chain information resolved by join resolver
   */
  public static class AutoJoinContext {
    private final Map<CubeDimensionTable, List<TableRelationship>> joinChain;
    private final Map<AbstractCubeTable, String> partialJoinConditions;
    private final Set<String> partitionPushedTables;
    private final boolean partialJoinChains;
    private final Map<AbstractCubeTable, JoinType> tableJoinTypeMap;
    private String mergedJoinClause;

    public AutoJoinContext(Map<CubeDimensionTable, List<TableRelationship>> joinChain,
        Map<AbstractCubeTable, String> partialJoinConditions,
        boolean partialJoinChains,
        Map<AbstractCubeTable, JoinType> tableJoinTypeMap) {
      this.joinChain = joinChain;
      this.partialJoinConditions = partialJoinConditions;
      partitionPushedTables = new HashSet<String>();
      this.partialJoinChains = partialJoinChains;
      this.tableJoinTypeMap = tableJoinTypeMap;
    }

    public Map<CubeDimensionTable, List<TableRelationship>> getJoinChain() {
      return joinChain;
    }

    public Map<AbstractCubeTable, String> getPartialJoinConditions() {
      return partialJoinConditions;
    }

    public String getMergedJoinClause(Configuration conf,
        Map<String, String> dimStorageTableToWhereClause,
        Map<AbstractCubeTable, Set<String>> storageTableToQuery,
        CubeQueryContext cubeql) {
      if (mergedJoinClause != null) {
        return mergedJoinClause;
      }

      for (List<TableRelationship> chain : joinChain.values()) {
        // Need to reverse the chain so that left most table in join comes first
        Collections.reverse(chain);
      }

      Set<String> clauses = new LinkedHashSet<String>();
      String joinTypeCfg = conf.get(CubeQueryConfUtil.JOIN_TYPE_KEY);
      String joinTypeStr = "";
      JoinType joinType = JoinType.INNER;

      // this flag is set to true if user has specified a partial join chain
      if (!partialJoinChains) {
        // User has not specified any join conditions. In this case, we rely on configuration for the join type
        if (StringUtils.isNotBlank(joinTypeCfg)) {
          joinType = JoinType.valueOf(joinTypeCfg.toUpperCase());
          joinTypeStr = getJoinTypeStr(joinType);
        }
      }

      for (Map.Entry<CubeDimensionTable, List<TableRelationship>> entry : joinChain.entrySet()) {
        List<TableRelationship> chain = entry.getValue();
        CubeDimensionTable table = entry.getKey();

        if (partialJoinChains) {
          joinType = tableJoinTypeMap.get(table);
          joinTypeStr = getJoinTypeStr(joinType);
        }

        for (TableRelationship rel : chain) {
          StringBuilder clause = new StringBuilder(joinTypeStr)
          .append(" join ");
          // Add storage table name followed by alias
          clause.append(cubeql.getStorageString(rel.getToTable()));

          clause.append(" on ")
          .append(cubeql.getAliasForTabName(rel.getFromTable().getName())).append(".").append(rel.getFromColumn())
          .append(" = ")
          .append(cubeql.getAliasForTabName(rel.getToTable().getName())).append(".").append(rel.getToColumn());

          // We have to push user specified filters for the joined tables
          String userFilter = null;
          // Partition condition on the tables also needs to be pushed depending on the join
          String storageFilter = null;

          if (JoinType.INNER == joinType || JoinType.LEFTOUTER == joinType || JoinType.LEFTSEMI == joinType) {
            // For inner and left joins push filter of right table
            userFilter = partialJoinConditions.get(rel.getToTable());
            if (partialJoinConditions.containsKey(rel.getFromTable())) {
              if (StringUtils.isNotBlank(userFilter)) {
                userFilter += (" AND " + partialJoinConditions.get(rel.getFromTable()));
              } else {
                userFilter = partialJoinConditions.get(rel.getFromTable());
              }
            }
            storageFilter = getStorageFilter(dimStorageTableToWhereClause, storageTableToQuery, rel.getToTable());
            partitionPushedTables.add(rel.getToTable().getName());
          } else if (JoinType.RIGHTOUTER == joinType) {
            // For right outer joins, push filters of left table
            userFilter = partialJoinConditions.get(rel.getFromTable());
            if (partialJoinConditions.containsKey(rel.getToTable())) {
              if (StringUtils.isNotBlank(userFilter)) {
                userFilter += (" AND " + partialJoinConditions.get(rel.getToTable()));
              } else {
                userFilter = partialJoinConditions.get(rel.getToTable());
              }
            }
            storageFilter = getStorageFilter(dimStorageTableToWhereClause, storageTableToQuery, rel.getFromTable());
            partitionPushedTables.add(rel.getFromTable().getName());
          } else if (JoinType.FULLOUTER == joinType) {
            // For full outer we need to push filters of both left and right tables in the join clause
            String leftFilter = null, rightFilter = null;
            String leftStorageFilter = null, rightStorgeFilter = null;

            if (StringUtils.isNotBlank(partialJoinConditions.get(rel.getFromTable()))) {
              leftFilter = partialJoinConditions.get(rel.getFromTable()) + " and ";
            }

            leftStorageFilter =
              getStorageFilter(dimStorageTableToWhereClause, storageTableToQuery, rel.getFromTable());
            if (StringUtils.isNotBlank((leftStorageFilter))) {
              leftStorageFilter += " and ";
              partitionPushedTables.add(rel.getFromTable().getName());
            }

            if (StringUtils.isNotBlank(partialJoinConditions.get(rel.getToTable()))) {
              rightFilter = partialJoinConditions.get(rel.getToTable());
            }

            rightStorgeFilter =
              getStorageFilter(dimStorageTableToWhereClause, storageTableToQuery, rel.getToTable());
            if (StringUtils.isNotBlank(rightStorgeFilter)) {
              partitionPushedTables.add(rel.getToTable().getName());
            }

            userFilter = (leftFilter == null ? "" : leftFilter) + (rightFilter == null ? "" : rightFilter);
            storageFilter = (leftStorageFilter == null ? "" : leftStorageFilter) +
              (rightStorgeFilter == null ? "" : rightStorgeFilter);
          }

          if (StringUtils.isNotBlank(userFilter)) {
            clause.append(" and (").append(userFilter).append(")");
          }
          if (StringUtils.isNotBlank(storageFilter)) {
            clause.append (" and (").append(storageFilter).append(")");
          }
          clauses.add(clause.toString());
        }
      }
      mergedJoinClause = StringUtils.join(clauses, " ");
      return mergedJoinClause;
    }

    private String getJoinTypeStr(JoinType joinType) {
      if (joinType == null) {
        return "";
      }
      switch (joinType) {
        case FULLOUTER: return "full outer";
        case INNER: return "inner";
        case LEFTOUTER: return "left outer";
        case LEFTSEMI: return "left semi";
        case UNIQUE: return "unique";
        case RIGHTOUTER: return "right outer";
        default: return "";
      }
    }

    private String getStorageFilter(Map<String, String> dimStorageTableToWhereClause,
                                    Map<AbstractCubeTable, Set<String>> storageTableToQuery,
                                    AbstractCubeTable table) {
      String whereClause = "";
      if (dimStorageTableToWhereClause != null && storageTableToQuery != null) {
        Set<String> queries = storageTableToQuery.get(table);
        if (queries != null) {
          String storageTableKey = queries.iterator().next();
          if (StringUtils.isNotBlank(storageTableKey)) {
            whereClause = dimStorageTableToWhereClause.get(storageTableKey);
          }
        }
      }
      return whereClause;
    }

    /**
     * Partition condition for these tables has been moved to the join clause
     * @return
     */
    public Set<String> getPushedPartitionTables() {
      return partitionPushedTables;
    }

  }

  private CubeMetastoreClient metastore;
  private final Map<AbstractCubeTable, String> partialJoinConditions;
  private final Map<AbstractCubeTable, JoinType> tableJoinTypeMap;
  private boolean partialJoinChain;
  private AbstractCubeTable target;
  private HiveConf conf;

  public JoinResolver(Configuration conf) {
    partialJoinConditions = new HashMap<AbstractCubeTable, String>();
    tableJoinTypeMap = new HashMap<AbstractCubeTable, JoinType>();
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
    boolean joinResolverDisabled = conf.getBoolean(
        CubeQueryConfUtil.DISABLE_AUTO_JOINS, CubeQueryConfUtil.DEFAULT_DISABLE_AUTO_JOINS);
    if (joinResolverDisabled) {
      if (cubeql.getJoinTree() != null) {
        cubeQB.setQbJoinTree(genJoinTree(cubeQB, cubeql.getJoinTree(), cubeql));
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

    cubeql.setAutoJoinTarget(target);
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
        if (LOG.isDebugEnabled()) {
          graph.print();
        }
        throw new SemanticException(ErrorMsg.NO_JOIN_PATH, joinee.getName(), target.getName());
      }
    }

    if (joinsResolved) {
      for (List<TableRelationship> chain : joinChain.values()) {
        for (TableRelationship rel : chain) {
          if (rel.getToTable() instanceof CubeDimensionTable) {
            autoJoinDims.add((CubeDimensionTable) rel.getToTable());
          }
          if (rel.getFromTable() instanceof  CubeDimensionTable) {
            autoJoinDims.add((CubeDimensionTable) rel.getFromTable());
          }
        }
      }
      AutoJoinContext joinCtx = new AutoJoinContext(joinChain, partialJoinConditions, partialJoinChain, tableJoinTypeMap);
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
      throw new SemanticException(ErrorMsg.JOIN_TARGET_NOT_CUBE_TABLE, targetTableName);
    }
  }

  private void searchDimensionTables(ASTNode node) throws HiveException {
    if (node == null) {
      return;
    }
    // User has specified join conditions partially. We need to store join conditions as well as join types
    partialJoinChain = true;
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
      tableJoinTypeMap.put(dimensionTable, getJoinType(node));
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

  private JoinType getJoinType(ASTNode node) {
    switch(node.getToken().getType()) {
      case TOK_LEFTOUTERJOIN:
        return JoinType.LEFTOUTER;
      case TOK_LEFTSEMIJOIN:
        return JoinType.LEFTSEMI;
      case TOK_RIGHTOUTERJOIN:
        return JoinType.RIGHTOUTER;
      case TOK_FULLOUTERJOIN:
        return JoinType.FULLOUTER;
      case TOK_JOIN:
        return JoinType.INNER;
      case TOK_UNIQUEJOIN:
        return JoinType.UNIQUE;
      default:
        return JoinType.INNER;
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
      new SemanticException(ErrorMsg.NO_JOIN_CONDITION_AVAIABLE);
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
