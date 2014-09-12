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


import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TMP_FILE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.cube.metadata.AbstractCubeTable;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeInterface;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.Dimension;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.JoinCond;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.codehaus.jackson.map.ObjectMapper;

public class CubeQueryContext {
  public static final String TIME_RANGE_FUNC = "time_range_in";
  public static final String NOW = "now";
  public static final String DEFAULT_TABLE = "_default_";
  public static Log LOG = LogFactory.getLog(CubeQueryContext.class.getName());
  private final ASTNode ast;
  private final QB qb;
  private String clauseName = null;
  private final HiveConf conf;

  private final List<TimeRange> timeRanges;

  // metadata
  private CubeInterface cube;
  // Dimensions accessed in the query
  protected Set<Dimension> dimensions =
      new HashSet<Dimension>();


  // Alias to table object mapping of tables accessed in this query
  private final Map<String, AbstractCubeTable> cubeTbls =
      new HashMap<String, AbstractCubeTable>();
  // Alias name to fields queried
  private final Map<String, Set<String>> tblAliasToColumns =
      new HashMap<String, Set<String>>();
  // Mapping of an expression to its column alias in the query
  private final Map<String, String> exprToAlias = new HashMap<String, String>();
  // All aggregate expressions in the query
  private final Set<String> aggregateExprs = new HashSet<String>();
  // Join conditions used in all join expressions
  private final Map<QBJoinTree, String> joinConds =
      new HashMap<QBJoinTree, String>();

  // storage specific
  protected final Set<CandidateFact> candidateFacts =
      new HashSet<CandidateFact>();
  protected final Map<Dimension, Set<CandidateDim>> candidateDims =
      new HashMap<Dimension, Set<CandidateDim>>();

  // query trees
  private String whereTree;
  private String havingTree;
  private String orderByTree;
  private String selectTree;
  private String groupByTree;
  private ASTNode havingAST;
  private ASTNode selectAST;
  private ASTNode whereAST;
  private ASTNode orderByAST;
  private ASTNode groupByAST;
  private CubeMetastoreClient client;
  private JoinResolver.AutoJoinContext autoJoinCtx;
  private Map<CubeFactTable, List<CandidateTablePruneCause>> factPruningMsgs = 
      new HashMap<CubeFactTable, List<CandidateTablePruneCause>>();
  private Map<Dimension, Map<CubeDimensionTable, List<CandidateTablePruneCause>>> dimPruningMsgs = 
      new HashMap<Dimension, Map<CubeDimensionTable, List<CandidateTablePruneCause>>>();

  public CubeQueryContext(ASTNode ast, QB qb, HiveConf conf)
      throws SemanticException {
    this.ast = ast;
    this.qb = qb;
    this.conf = conf;
    this.clauseName = getClause();
    this.timeRanges = new ArrayList<TimeRange>();
    try {
      client = CubeMetastoreClient.getInstance(conf);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
    if (qb.getParseInfo().getWhrForClause(clauseName) != null) {
      this.whereTree = HQLParser.getString(
          qb.getParseInfo().getWhrForClause(clauseName));
      this.whereAST = qb.getParseInfo().getWhrForClause(clauseName);
    }
    if (qb.getParseInfo().getHavingForClause(clauseName) != null) {
      this.havingTree = HQLParser.getString(qb.getParseInfo().getHavingForClause(
          clauseName));
      this.havingAST = qb.getParseInfo().getHavingForClause(
          clauseName);
    }
    if (qb.getParseInfo().getOrderByForClause(clauseName) != null) {
      this.orderByTree = HQLParser.getString(qb.getParseInfo()
          .getOrderByForClause(clauseName));
      this.orderByAST = qb.getParseInfo().getOrderByForClause(clauseName);
    }
    if (qb.getParseInfo().getGroupByForClause(clauseName) != null) {
      this.groupByTree = HQLParser.getString(qb.getParseInfo()
          .getGroupByForClause(clauseName));
      this.groupByAST = qb.getParseInfo().getGroupByForClause(clauseName);
    }
    if (qb.getParseInfo().getSelForClause(clauseName) != null) {
      this.selectTree = HQLParser.getString(qb.getParseInfo().getSelForClause(
          clauseName));
      this.selectAST = qb.getParseInfo().getSelForClause(
          clauseName);
    }

    for (ASTNode aggrTree : qb.getParseInfo().getAggregationExprsForClause(
        clauseName).values()) {
      String aggr = HQLParser.getString(aggrTree);
      aggregateExprs.add(aggr);
    }

    extractMetaTables();
  }

  public boolean hasCubeInQuery() {
    return cube != null;
  }

  public boolean hasDimensionInQuery() {
    return dimensions != null && !dimensions.isEmpty();
  }

  private void extractMetaTables() throws SemanticException {
    try {
      List<String> tabAliases = new ArrayList<String>(qb.getTabAliases());
      for (String alias : tabAliases) {
        addQueriedTable(alias);
      }
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  public void addQueriedTable(String alias) throws SemanticException {
    alias = alias.toLowerCase();
    if (cubeTbls.containsKey(alias)) {
      return;
    }
    String tblName = qb.getTabNameForAlias(alias);
    if (tblName == null) {
      tblName = alias;
    }
    try {
      if (client.isCube(tblName)) {
        if (cube != null) {
          if (!cube.getName().equalsIgnoreCase(tblName)) {
            throw new SemanticException(ErrorMsg.MORE_THAN_ONE_CUBE, cube.getName(), tblName);
          }
        }
        cube = client.getCube(tblName);
        if (!cube.canBeQueried()) {
          throw new SemanticException(ErrorMsg.CUBE_NOT_QUERYABLE, tblName);
        }
        cubeTbls.put(alias, (AbstractCubeTable)cube);
      } else if (client.isDimension(tblName)) {
        Dimension dim = client.getDimension(tblName);
        dimensions.add(dim);
        cubeTbls.put(alias, dim);
      } else {
        throw new SemanticException(ErrorMsg.NEITHER_CUBE_NOR_DIMENSION);
      }
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  private String getClause() {
    if (clauseName == null) {
      TreeSet<String> ks = new TreeSet<String>(
          qb.getParseInfo().getClauseNames());
      clauseName = ks.first();
    }
    return clauseName;
  }

  public CubeInterface getCube() {
    return cube;
  }

  public QB getQB() {
    return qb;
  }

  public Set<CandidateFact> getCandidateFactTables() {
    return candidateFacts;
  }

  public Map<Dimension, Set<CandidateDim>> getCandidateDimTables() {
    return candidateDims;
  }

  public Map<CubeFactTable, List<CandidateTablePruneCause>> getFactPruningMsgs() {
    return factPruningMsgs;
  }

  public void addFactPruningMsgs(CubeFactTable fact, CandidateTablePruneCause factPruningMsg) {
    List<CandidateTablePruneCause> pruneMsgs = factPruningMsgs.get(fact);
    if (pruneMsgs == null) {
      pruneMsgs = new ArrayList<CandidateTablePruneCause>();
      factPruningMsgs.put(fact, pruneMsgs);
    }
    pruneMsgs.add(factPruningMsg);
  }

  public void addDimPruningMsgs(Dimension dim, CubeDimensionTable dimtable,
      CandidateTablePruneCause msg) {
    Map<CubeDimensionTable, List<CandidateTablePruneCause>> dimMsgs = dimPruningMsgs.get(dimtable);
    if (dimMsgs == null) {
      dimMsgs = new HashMap<CubeDimensionTable, List<CandidateTablePruneCause>>();
      dimPruningMsgs.put(dim, dimMsgs);
    }
    List<CandidateTablePruneCause> pruneMsgs = dimMsgs.get(dimtable);
    if (pruneMsgs == null) {
      pruneMsgs = new ArrayList<CandidateTablePruneCause>();
      dimMsgs.put(dimtable, pruneMsgs);
    }
    pruneMsgs.add(msg);
  }

  public Set<Dimension> getDimensions() {
    return dimensions;
  }

  public String getAliasForTabName(String tabName) {
    for (String alias : qb.getTabAliases()) {
      String table = qb.getTabNameForAlias(alias);
      if (table != null && table.equalsIgnoreCase(tabName)) {
        return alias;
      }
    }
    return tabName;
  }

  public void print() {
    StringBuilder builder = new StringBuilder();
    builder.append("ASTNode:" + ast.dump() + "\n");
    builder.append("QB:");
    builder.append("\n numJoins:" + qb.getNumJoins());
    builder.append("\n numGbys:" + qb.getNumGbys());
    builder.append("\n numSels:" + qb.getNumSels());
    builder.append("\n numSelDis:" + qb.getNumSelDi());
    builder.append("\n aliasToTabs:");
    Set<String> tabAliases = qb.getTabAliases();
    for (String alias : tabAliases) {
      builder.append("\n\t" + alias + ":" + qb.getTabNameForAlias(alias));
    }
    builder.append("\n aliases:");
    for (String alias : qb.getAliases()) {
      builder.append(alias);
      builder.append(", ");
    }
    builder.append("id:" + qb.getId());
    builder.append("isQuery:" + qb.getIsQuery());
    builder.append("\n QBParseInfo");
    QBParseInfo parseInfo = qb.getParseInfo();
    builder.append("\n isSubQ: " + parseInfo.getIsSubQ());
    builder.append("\n alias: " + parseInfo.getAlias());
    if (parseInfo.getJoinExpr() != null) {
      builder.append("\n joinExpr: " + parseInfo.getJoinExpr().dump());
    }
    builder.append("\n hints: " + parseInfo.getHints());
    builder.append("\n aliasToSrc: ");
    for (String alias : tabAliases) {
      builder.append("\n\t" + alias + ": " + parseInfo.getSrcForAlias(alias)
          .dump());
    }
    TreeSet<String> clauses = new TreeSet<String>(parseInfo.getClauseNames());
    for (String clause : clauses) {
      builder.append("\n\t" + clause + ": " + parseInfo
          .getClauseNamesForDest());
    }
    String clause = clauses.first();
    if (parseInfo.getWhrForClause(clause) != null) {
      builder.append("\n whereexpr: " + parseInfo.getWhrForClause(clause)
          .dump());
    }
    if (parseInfo.getGroupByForClause(clause) != null) {
      builder.append("\n groupby expr: " + parseInfo.getGroupByForClause(clause)
          .dump());
    }
    if (parseInfo.getSelForClause(clause) != null) {
      builder.append("\n sel expr: " + parseInfo.getSelForClause(clause)
          .dump());
    }
    if (parseInfo.getHavingForClause(clause) != null) {
      builder.append("\n having expr: " + parseInfo.getHavingForClause(clause)
          .dump());
    }
    if (parseInfo.getDestLimit(clause) != null) {
      builder.append("\n limit: " + parseInfo.getDestLimit(clause));
    }
    if (parseInfo.getAllExprToColumnAlias() != null
        && !parseInfo.getAllExprToColumnAlias().isEmpty()) {
      builder.append("\n exprToColumnAlias:");
      for (Map.Entry<ASTNode, String> entry : parseInfo
          .getAllExprToColumnAlias().entrySet()) {
        builder.append("\n\t expr: " + entry.getKey().dump()
            + " ColumnAlias: " + entry.getValue());
      }
    }
    if (parseInfo.getAggregationExprsForClause(clause) != null) {
      builder.append("\n aggregateexprs:");
      for (Map.Entry<String, ASTNode> entry : parseInfo
          .getAggregationExprsForClause(clause).entrySet()) {
        builder.append("\n\t key: " + entry.getKey() + " expr: " +
            entry.getValue().dump());
      }
    }
    if (parseInfo.getDistinctFuncExprsForClause(clause) != null) {
      builder.append("\n distinctFuncExprs:");
      for (ASTNode entry : parseInfo.getDistinctFuncExprsForClause(clause)) {
        builder.append("\n\t expr: " + entry.dump());
      }
    }

    if (qb.getQbJoinTree() != null) {
      builder.append("\n\n JoinTree");
      QBJoinTree joinTree = qb.getQbJoinTree();
      printJoinTree(joinTree, builder);
    }

    if (qb.getParseInfo().getDestForClause(clause) != null) {
      builder.append("\n Destination:");
      builder.append("\n\t dest expr:" +
          qb.getParseInfo().getDestForClause(clause).dump());
    }
    LOG.info(builder.toString());
  }

  void printJoinTree(QBJoinTree joinTree, StringBuilder builder) {
    builder.append("leftAlias:" + joinTree.getLeftAlias());
    if (joinTree.getLeftAliases() != null) {
      builder.append("\n leftAliases:");
      for (String alias : joinTree.getLeftAliases()) {
        builder.append("\n\t " + alias);
      }
    }
    if (joinTree.getRightAliases() != null) {
      builder.append("\n rightAliases:");
      for (String alias : joinTree.getRightAliases()) {
        builder.append("\n\t " + alias);
      }
    }
    if (joinTree.getJoinSrc() != null) {
      builder.append("\n JoinSrc: {");
      printJoinTree(joinTree.getJoinSrc(), builder);
      builder.append("\n }");
    }
    if (joinTree.getBaseSrc() != null) {
      builder.append("\n baseSrcs:");
      for (String src : joinTree.getBaseSrc()) {
        builder.append("\n\t " + src);
      }
    }
    builder.append("\n noOuterJoin: " + joinTree.getNoOuterJoin());
    builder.append("\n noSemiJoin: " + joinTree.getNoSemiJoin());
    builder.append("\n mapSideJoin: " + joinTree.isMapSideJoin());
    if (joinTree.getJoinCond() != null) {
      builder.append("\n joinConds:");
      for (JoinCond cond : joinTree.getJoinCond()) {
        builder.append("\n\t left: " + cond.getLeft() + " right: " +
            cond.getRight() + " type:" + cond.getJoinType() +
            " preserved:" + cond.getPreserved());
      }
    }
  }

  public String getSelectTree() {
    return selectTree;
  }

  public String getWhereTree() {
    return whereTree;
  }

  public String getGroupByTree() {
    return groupByTree;
  }

  public String getHavingTree() {
    return havingTree;
  }

  public ASTNode getJoinTree() {
    return qb.getParseInfo().getJoinExpr();
  }

  public QBJoinTree getQBJoinTree() {
    return qb.getQbJoinTree();
  }

  public String getOrderByTree() {
    return orderByTree;
  }

  public Integer getLimitValue() {
    return qb.getParseInfo().getDestLimit(getClause());
  }

  private String getStorageStringWithAlias(CandidateFact fact,
      Map<Dimension, CandidateDim> dimsToQuery, String alias) {
    if (cubeTbls.get(alias) instanceof CubeInterface) {
      return fact.getStorageString(alias);
    } else {
      return dimsToQuery.get(cubeTbls.get(alias)).getStorageString(alias);
    }
  }

  private String getWhereClauseWithAlias(Map<Dimension, CandidateDim> dimsToQuery,
      String alias) {
    Dimension dim = (Dimension)cubeTbls.get(alias);
    return dimsToQuery.get(dim).whereClause.replace(
        getAliasForTabName(dim.getName()), alias);
  }
  String getQBFromString(CandidateFact fact,
      Map<Dimension, CandidateDim> dimsToQuery) throws SemanticException {
    String fromString = null;
    if (getJoinTree() == null) {
      if (cube != null) {
        fromString = fact.getStorageString(getAliasForTabName(cube.getName())) ;
      } else {
        if (dimensions.size() != 1) {
          throw new SemanticException(ErrorMsg.NO_JOIN_CONDITION_AVAIABLE);
        }
        Dimension dim = dimensions.iterator().next(); 
        fromString = dimsToQuery.get(dim).getStorageString(getAliasForTabName(dim.getName()));
      }
    } else {
      StringBuilder builder = new StringBuilder();
      getQLString(qb.getQbJoinTree(), builder, fact, dimsToQuery);
      fromString = builder.toString();
    }
    return fromString;
  }

  private void getQLString(QBJoinTree joinTree, StringBuilder builder, CandidateFact fact,
      Map<Dimension, CandidateDim> dimsToQuery)
      throws SemanticException {
    String joiningTable = null;
    if (joinTree.getBaseSrc()[0] == null) {
      if (joinTree.getJoinSrc() != null) {
        getQLString(joinTree.getJoinSrc(), builder, fact, dimsToQuery);
      }
    } else { // (joinTree.getBaseSrc()[0] != null){
      String alias = joinTree.getBaseSrc()[0].toLowerCase();
      builder.append(getStorageStringWithAlias(fact, dimsToQuery, alias));
      if (joinTree.getJoinCond()[0].getJoinType().equals(JoinType.RIGHTOUTER)) {
        joiningTable = alias;
      }
    }
    if (joinTree.getJoinCond() != null) {
      builder.append(JoinResolver.getJoinTypeStr(joinTree.getJoinCond()[0].getJoinType()));
      builder.append(" JOIN ");
    }
    if (joinTree.getBaseSrc()[1] == null) {
      if (joinTree.getJoinSrc() != null) {
        getQLString(joinTree.getJoinSrc(), builder, fact, dimsToQuery);
      }
    } else { // (joinTree.getBaseSrc()[1] != null){
      String alias = joinTree.getBaseSrc()[1].toLowerCase();
      builder.append(getStorageStringWithAlias(fact, dimsToQuery, alias));
      if (joinTree.getJoinCond()[0].getJoinType().equals(JoinType.LEFTOUTER)) {
        joiningTable = alias;
      }
    }

    String joinCond = joinConds.get(joinTree);
    if (joinCond != null) {
      builder.append(" ON ");
      builder.append(joinCond);
      if (joiningTable != null) {
        // assuming the joining table to be dimension table
        HQLContext.appendWhereClause(builder,
            getWhereClauseWithAlias(dimsToQuery, joiningTable), true);
        dimsToQuery.get(cubeTbls.get(joiningTable)).setWhereClauseAdded();
      }
    } else {
      throw new SemanticException(ErrorMsg.NO_JOIN_CONDITION_AVAIABLE);
    }
  }

  void setNonexistingParts(Map<String, List<String>> nonExistingParts) throws SemanticException {
    if (!nonExistingParts.isEmpty()) {
      ByteArrayOutputStream out = null;
      String partsStr;
      try {
        ObjectMapper mapper = new ObjectMapper();
        out = new ByteArrayOutputStream();
        mapper.writeValue(out, nonExistingParts);
        partsStr = out.toString("UTF-8");
      } catch (Exception e) {
        throw new SemanticException("Error writing non existing parts", e);
      } finally {
        if (out != null) {
          try {
            out.close();
          } catch (IOException e) {
            throw new SemanticException(e);
          }
        }
      }
      conf.set(CubeQueryConfUtil.NON_EXISTING_PARTITIONS, partsStr);
    } else {
      conf.unset(CubeQueryConfUtil.NON_EXISTING_PARTITIONS);
    }
  }

  public String getNonExistingParts() {
    return conf.get(CubeQueryConfUtil.NON_EXISTING_PARTITIONS);
  }

  private Map<Dimension, CandidateDim> pickCandidateDimsToQuery()
      throws SemanticException {
    Map<Dimension, CandidateDim> dimsToQuery = null;
    if (!dimensions.isEmpty()) {
      dimsToQuery = new HashMap<Dimension, CandidateDim>();
      for (Dimension dim : dimensions) {
        if (candidateDims.get(dim).size() > 0) {
          CandidateDim cdim = candidateDims.get(dim).iterator().next();
          LOG.info("Available candidate dims are:" + candidateDims.get(dim) +
              ", picking up " + cdim.dimtable  + " for querying");
          dimsToQuery.put(dim, cdim);
        } else {
          String reason = "";
          if (!dimPruningMsgs.get(dim).isEmpty()) {
            ByteArrayOutputStream out = null;
            try {
              ObjectMapper mapper = new ObjectMapper();
              out = new ByteArrayOutputStream();
              mapper.writeValue(out, dimPruningMsgs.get(dim).values());
              reason = out.toString("UTF-8");
            } catch (Exception e) {
              throw new SemanticException("Error writing non existing parts", e);
            } finally {
              if (out != null) {
                try {
                  out.close();
                } catch (IOException e) {
                  throw new SemanticException(e);
                }
              }
            }
          }
          throw new SemanticException(ErrorMsg.NO_CANDIDATE_DIM_AVAILABLE, dim.getName(), reason);
        }
      }
    }

    return dimsToQuery;
  }

  private CandidateFact pickCandidateFactToQuery() throws SemanticException {
    CandidateFact fact = null;
    if (hasCubeInQuery()) {
      if (candidateFacts.size() > 0) {
        fact = candidateFacts.iterator().next();
        LOG.info("Available candidate facts:" + candidateFacts +
            ", picking up " + fact.fact + " for querying");
      } else {
        String reason = "";
        if (!factPruningMsgs.isEmpty()) {
          ByteArrayOutputStream out = null;
          try {
            ObjectMapper mapper = new ObjectMapper();
            out = new ByteArrayOutputStream();
            mapper.writeValue(out, factPruningMsgs.values());
            reason = out.toString("UTF-8");
          } catch (Exception e) {
            throw new SemanticException("Error writing non existing parts", e);
          } finally {
            if (out != null) {
              try {
                out.close();
              } catch (IOException e) {
                throw new SemanticException(e);
              }
            }
          }
        }
        throw new SemanticException(ErrorMsg.NO_CANDIDATE_FACT_AVAILABLE, reason);
      }
    }

    if (fact != null) {
      if (fact.storageTables.isEmpty()) {
        // would never reach here. This fact would have been removed from
        // candidates by storage table resolver
        throw new SemanticException(ErrorMsg.NO_STORAGE_TABLE_AVAIABLE, fact.toString());
      }
      if (fact.storageTables != null && fact.storageTables.size() > 1
          && !fact.enabledMultiTableSelect) {
        throw new SemanticException(ErrorMsg.MULTIPLE_STORAGE_TABLES);
      }
    }
    return fact;
  }

  private HQLContext hqlContext;
  public String toHQL() throws SemanticException {
    hqlContext = new HQLContext(pickCandidateFactToQuery(), pickCandidateDimsToQuery(), this);
    return hqlContext.toHQL();
  }

  public ASTNode toAST(Context ctx) throws SemanticException {
    String hql = toHQL();
    ParseDriver pd = new ParseDriver();
    ASTNode tree;
    try {
      LOG.info("HQL:" + hql);
      System.out.println("Rewritten HQL:" + hql);
      tree = pd.parse(hql, ctx);
    } catch (ParseException e) {
      throw new SemanticException(e);
    }
    return ParseUtils.findRootNonNullToken(tree);
  }

  public Map<String, Set<String>> getTblAlaisToColumns() {
    return tblAliasToColumns;
  }

  public Set<String> getColumnsQueried(String tblName) {
    return tblAliasToColumns.get(getAliasForTabName(tblName));
  }

  public void addColumnsQueried(AbstractCubeTable table, String column) {
    addColumnsQueried(getAliasForTabName(table.getName()), column);
  }

  public void addColumnsQueried(String alias, String column) {
    Set<String> cols = tblAliasToColumns.get(alias.toLowerCase());
    if (cols == null) {
      cols = new HashSet<String>();
      tblAliasToColumns.put(alias.toLowerCase(), cols);
    }
    cols.add(column);
  }

  public void setSelectTree(String selectTree) {
    this.selectTree = selectTree;
  }

  public void setWhereTree(String whereTree) {
    this.whereTree = whereTree;
  }

  public void setHavingTree(String havingTree) {
    this.havingTree = havingTree;
  }

  public void setGroupByTree(String groupByTree) {
    this.groupByTree = groupByTree;
  }

  public void setOrderByTree(String orderByTree) {
    this.orderByTree = orderByTree;
  }

  public boolean isCubeMeasure(String col) {
    if (col == null) {
      return false;
    }

    col = col.trim();
    // Take care of brackets added around col names in HQLParsrer.getString
    if (col.startsWith("(") && col.endsWith(")") && col.length() > 2) {
      col = col.substring(1, col.length() -1);
    }

    String[] split = StringUtils.split(col, ".");
    if (split.length <= 1) {
      return cube.getMeasureNames().contains(col.trim().toLowerCase());
    } else {
      String cubeName = split[0].trim();
      String colName = split[1].trim();
      if (cubeName.equalsIgnoreCase(cube.getName()) ||
          cubeName.equalsIgnoreCase(getAliasForTabName(cube.getName()))) {
        return cube.getMeasureNames().contains(colName.toLowerCase());
      } else {
        return false;
      }
    }
  }

  public boolean isAggregateExpr(String expr) {
    return aggregateExprs.contains(expr == null ? expr : expr.toLowerCase());
  }

  public boolean hasAggregates() {
    return !aggregateExprs.isEmpty();
  }

  public String getAlias(String expr) {
    return exprToAlias.get(expr);
  }

  public Map<String, String> getExprToAliasMap() {
    return exprToAlias;
  }

  public void addAggregateExpr(String expr) {
    aggregateExprs.add(expr);
  }

  public Set<String> getAggregateExprs() {
    return aggregateExprs;
  }

  public ASTNode getHavingAST() {
    return havingAST;
  }

  public ASTNode getSelectAST() {
    return selectAST;
  }

  public Map<QBJoinTree, String> getJoinConds() {
    return joinConds;
  }

  public void setJoinCond(QBJoinTree qb, String cond) {
    joinConds.put(qb, cond);
  }

  public ASTNode getWhereAST() {
    return whereAST;
  }

  public ASTNode getOrderByAST() {
    return orderByAST;
  }

  public ASTNode getGroupByAST() {
    return groupByAST;
  }

  public AbstractCubeTable getQueriedTable(String alias) {
    if (cube != null && cube.getName().equalsIgnoreCase(qb.getTabNameForAlias((alias)))) {
      return (AbstractCubeTable)cube;
    }
    for (Dimension dim : dimensions) {
      if (dim.getName().equalsIgnoreCase(qb.getTabNameForAlias(alias))) {
        return dim;
      }
    }
    return null;
  }

  public String getInsertClause() {
    String insertString = "";
    ASTNode destTree = qb.getParseInfo().getDestForClause(clauseName);
    if (destTree != null && ((ASTNode)(destTree.getChild(0)))
        .getToken().getType() != TOK_TMP_FILE) {
      insertString = "INSERT OVERWRITE" + HQLParser.getString(
          qb.getParseInfo().getDestForClause(clauseName));
    }
    return insertString;
  }

  public CubeMetastoreClient getMetastoreClient() {
    return client;
  }

  public void addExprToAlias(ASTNode expr, ASTNode alias) {
    exprToAlias.put(HQLParser.getString(expr).trim(),
        alias.getText().toLowerCase());
  }

  public List<TimeRange> getTimeRanges() {
    return timeRanges;
  }

  public HiveConf getHiveConf() {
    return conf;
  }

  public void setAutoJoinCtx(JoinResolver.AutoJoinContext autoJoinCtx) {
    this.autoJoinCtx = autoJoinCtx;
  }

  public JoinResolver.AutoJoinContext getAutoJoinCtx() {
    return autoJoinCtx;
  }

  /**
   * @return the hqlContext
   */
  public HQLContext getHqlContext() {
    return hqlContext;
  }

  public boolean shouldReplaceTimeDimWithPart() {
    return getHiveConf().getBoolean(CubeQueryConfUtil.REPLACE_TIMEDIM_WITH_PART_COL,
      CubeQueryConfUtil.DEFAULT_REPLACE_TIMEDIM_WITH_PART_COL);
  }

  public String getPartitionColumnOfTimeDim(String timeDimName) {
    if (!hasCubeInQuery()) {
      return timeDimName;
    }

    AbstractCubeTable cube = (AbstractCubeTable) getCube();
    String partCol = cube.getProperties().get(CubeQueryConfUtil.TIMEDIM_TO_PART_MAPPING_PFX + timeDimName);
    return StringUtils.isNotBlank(partCol) ? partCol : timeDimName;
  }

  public String getTimeDimOfPartitionColumn(String partCol) {
    if (!hasCubeInQuery()) {
      return partCol;
    }

    AbstractCubeTable cube = (AbstractCubeTable) getCube();
    Map<String, String> properties = cube.getProperties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (entry.getKey().startsWith(CubeQueryConfUtil.TIMEDIM_TO_PART_MAPPING_PFX )
        && entry.getValue().equalsIgnoreCase(partCol)) {
        String timeDim = entry.getKey().replace(CubeQueryConfUtil.TIMEDIM_TO_PART_MAPPING_PFX , "");
        return timeDim;
      }
    }
    return partCol;
  }

  static interface CandidateTable {
    public String getStorageString(String alias);
  }
  static class CandidateFact implements CandidateTable {
    final CubeFactTable fact;
    Set<String> storageTables;
    boolean enabledMultiTableSelect;
    int numQueriedParts = 0;
    final Map<TimeRange, String> rangeToWhereClause = new HashMap<TimeRange, String>();
    private boolean dbResolved = false;
    CandidateFact(CubeFactTable fact) {
      this.fact = fact;
    }

    @Override
    public String toString() {
      return fact.toString();
    }
    
    public String getStorageString(String alias) {
      if (!dbResolved) {
        String database = SessionState.get().getCurrentDatabase();
        // Add database name prefix for non default database
        if (StringUtils.isNotBlank(database) && !"default".equalsIgnoreCase(database)) {
          Set<String> storageTbls = new HashSet<String>();
          Iterator<String> names = storageTables.iterator();
          while (names.hasNext()) {
            storageTbls.add(database + "." + names.next());
          }
          this.storageTables = storageTbls;
        }
        dbResolved = true;
      }
      return StringUtils.join(storageTables, ",") + " " + alias;

    }
  }

  static class CandidateDim implements CandidateTable {
    final CubeDimensionTable dimtable;
    String storageTable;
    String whereClause;
    private boolean dbResolved = false;
    private boolean whereClauseAdded = false;

    public boolean isWhereClauseAdded() {
      return whereClauseAdded;
    }

    public void setWhereClauseAdded() {
      this.whereClauseAdded = true;
    }

    CandidateDim(CubeDimensionTable dim) {
      this.dimtable = dim;
    }

    @Override
    public String toString() {
      return dimtable.toString();
    }

    public String getStorageString(String alias) {
      if (!dbResolved) {
        String database = SessionState.get().getCurrentDatabase();
        // Add database name prefix for non default database
        if (StringUtils.isNotBlank(database) && !"default".equalsIgnoreCase(database)) {
          storageTable = database + "." + storageTable;
        }
        dbResolved = true;
      }
      return storageTable + " " + alias;
    }
    
  }
}
