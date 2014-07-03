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


import static org.apache.hadoop.hive.ql.parse.HiveParser.DOT;
import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_ALLCOLREF;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FUNCTION;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FUNCTIONSTAR;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_SELEXPR;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABLE_OR_COL;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TMP_FILE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
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
import org.apache.hadoop.hive.ql.cube.metadata.CubeColumn;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeInterface;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.MetastoreUtil;
import org.apache.hadoop.hive.ql.cube.metadata.Dimension;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser.ASTNodeVisitor;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser.TreeNode;
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
import org.apache.hadoop.hive.ql.plan.PlanUtils;
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
  // All measures in this cube
  private Set<String> cubeMeasureNames;
  // All dimensions in this cube
  private Set<String> cubeDimNames;
  // Dimensions used to decide partitions
  private Set<String> timedDimensions;
  // Dimensions accessed in the query
  protected Set<Dimension> dimensions =
      new HashSet<Dimension>();

  // Dimension table accessed but not in from
  protected Set<Dimension> autoJoinDims = new HashSet<Dimension>();

  // Name to table object mapping of tables accessed in this query
  private final Map<String, AbstractCubeTable> cubeTbls =
      new HashMap<String, AbstractCubeTable>();
  // Mapping of table objects to all columns of that table accessed in the query
  private final Map<AbstractCubeTable, Set<String>> cubeTabToCols =
      new HashMap<AbstractCubeTable, Set<String>>();

  // Alias name to fields queried
  private final Map<String, Set<String>> tblAliasToColumns =
      new HashMap<String, Set<String>>();
  // All columns accessed in this query
  private final Set<String> cubeColumnsQueried = new HashSet<String>();
  // Mapping of a qualified column name to its table alias
  private final Map<String, String> columnToTabAlias =
      new HashMap<String, String>();
  // Mapping of an expression to the columns within that expression
  private final Map<CubeQueryExpr, Set<String>> exprToCols =
      new HashMap<CubeQueryExpr, Set<String>>();

  // Mapping of an expression to its column alias in the query
  private final Map<String, String> exprToAlias = new HashMap<String, String>();
  // Columns inside aggregate expressions in the query
  private final Set<String> aggregateCols = new HashSet<String>();
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

    extractMetaTables();
    extractTimeRange();
    extractColumns();
    extractTabAliasForCol();
    doColLifeValidation();
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
        String tblName = qb.getTabNameForAlias(alias);
        if (client.isCube(tblName)) {
          if (cube != null) {
            if (cube.getName() != tblName) {
              throw new SemanticException(ErrorMsg.MORE_THAN_ONE_CUBE);
            }
          }
          cube = client.getCube(tblName);
          if (!cube.canBeQueried()) {
            throw new SemanticException(ErrorMsg.CUBE_NOT_QUERYABLE, tblName);
          }
          cubeMeasureNames = cube.getMeasureNames();
          cubeDimNames = cube.getDimKeyNames();
          timedDimensions = cube.getTimedDimensions();
          Set<String> cubeCols = new HashSet<String>();
          cubeCols.addAll(cubeMeasureNames);
          cubeCols.addAll(cubeDimNames);
          if (timedDimensions != null) {
            cubeCols.addAll(timedDimensions);
          }
          cubeTabToCols.put((AbstractCubeTable)cube, cubeCols);
          cubeTbls.put(tblName.toLowerCase(), (AbstractCubeTable)cube);
        } else if (client.isDimension(tblName)) {
          Dimension dim = client.getDimension(tblName);
          dimensions.add(dim);
          cubeTabToCols.put(dim, MetastoreUtil.getAttributeNames(dim));
          cubeTbls.put(tblName.toLowerCase(), dim);
        }
      }
      if (cube == null && dimensions.size() == 0) {
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

  private void extractTimeRange() throws SemanticException {
    if (cube == null) {
      return;
    }
    // get time range -
    // Time range should be direct child of where condition
    // TOK_WHERE.TOK_FUNCTION.Identifier Or, it should be right hand child of
    // AND condition TOK_WHERE.KW_AND.TOK_FUNCTION.Identifier
    ASTNode whereTree = qb.getParseInfo().getWhrForClause(getClause());
    if (whereTree == null || whereTree.getChildCount() < 1) {
      throw new SemanticException(ErrorMsg.NO_TIMERANGE_FILTER);
    }
    searchTimeRanges(whereTree);
  }

  private void searchTimeRanges(ASTNode root) throws SemanticException {
    if (root == null) {
      return;
    } else if (root.getToken().getType() == TOK_FUNCTION) {
      ASTNode fname = HQLParser.findNodeByPath(root, Identifier);
      if (fname != null && TIME_RANGE_FUNC.equalsIgnoreCase(fname.getText())) {
        processTimeRangeFunction(root, null);
      }
    } else {
      for (int i = 0; i < root.getChildCount(); i++) {
        ASTNode child = (ASTNode) root.getChild(i);
        searchTimeRanges(child);
      }
    }
  }

  private void processTimeRangeFunction(ASTNode timenode, TimeRange parent) throws SemanticException {
    TimeRange.TimeRangeBuilder builder = TimeRange.getBuilder();
    builder.astNode(timenode);

    String timeDimName = PlanUtils.stripQuotes(timenode.getChild(1).getText());
    if (cube.getTimedDimensions().contains(timeDimName)) {
      builder.partitionColumn(timeDimName);
    } else {
      throw new SemanticException(ErrorMsg.NOT_A_TIMED_DIMENSION, timeDimName);
    }

    String fromDateRaw = PlanUtils.stripQuotes(timenode.getChild(2).getText());
    String toDateRaw = null;
    if (timenode.getChildCount() > 3) {
      ASTNode toDateNode = (ASTNode) timenode.getChild(3);
      if (toDateNode != null) {
        toDateRaw = PlanUtils.stripQuotes(timenode.getChild(3).getText());
      }
    }

    Date now = new Date();
    builder.fromDate(DateUtil.resolveDate(fromDateRaw, now));
    if (StringUtils.isNotBlank(toDateRaw)) {
      builder.toDate(DateUtil.resolveDate(toDateRaw, now));
    } else {
      builder.toDate(now);
    }

    TimeRange range = builder.build();
    range.validate();

    timeRanges.add(range);
  }

  private void extractColumns() throws SemanticException {
    // Check if its 'select * from...'
    ASTNode selTree = qb.getParseInfo().getSelForClause(clauseName);
    if (selTree.getChildCount() == 1) {
      ASTNode star = HQLParser.findNodeByPath(selTree, TOK_SELEXPR,
          TOK_ALLCOLREF);
      if (star == null) {
        star = HQLParser.findNodeByPath(selTree, TOK_SELEXPR,
            TOK_FUNCTIONSTAR);
      }

      if (star != null) {
        int starType = star.getToken().getType();
        if (TOK_FUNCTIONSTAR == starType || TOK_ALLCOLREF == starType) {
          throw new SemanticException(ErrorMsg.ALL_COLUMNS_NOT_SUPPORTED);
        }
      }
    }

    for (CubeQueryExpr expr : CubeQueryExpr.values()) {
      Set<String> columns = new HashSet<String>();
      exprToCols.put(expr, columns);
      getColsForTree(getExprTree(expr), columns, tblAliasToColumns, exprToAlias);
    }

    for (ASTNode aggrTree : qb.getParseInfo().getAggregationExprsForClause(
        clauseName).values()) {
      getColsForTree(aggrTree, aggregateCols, null, null);
      String aggr = HQLParser.getString(aggrTree);
      aggregateExprs.add(aggr);
    }
    if (cube != null) {
      String cubeAlias = getAliasForTabName(cube.getName());
      if (tblAliasToColumns.get(cubeAlias) != null) {
        cubeColumnsQueried.addAll(tblAliasToColumns.get(cubeAlias));
      }
    }

    // Update auto join dimension tables
    autoJoinDims.addAll(dimensions);
    for (String table : tblAliasToColumns.keySet()) {
      try {
        if (!DEFAULT_TABLE.equalsIgnoreCase(table)) {
          if (qb.getTabNameForAlias(table) != null) {
            table = qb.getTabNameForAlias(table);
          }
          if (client.isDimension(table)) {
            Dimension dimTable = client.getDimension(table);
            autoJoinDims.add(dimTable);
          }
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      }
    }
  }

  private ASTNode getExprTree(CubeQueryExpr expr) {
    switch (expr) {
    case SELECT:
      return qb.getParseInfo().getSelForClause(clauseName);
    case WHERE:
      return qb.getParseInfo().getWhrForClause(clauseName);
    case HAVING:
      return qb.getParseInfo().getHavingForClause(clauseName);
    case GROUPBY:
      return qb.getParseInfo().getGroupByForClause(clauseName);
    case ORDERBY:
      qb.getParseInfo().getOrderByForClause(clauseName);
    case JOIN:
      return qb.getParseInfo().getJoinExpr();
    default:
      return null;
    }
  }

  private static void getColsForTree(ASTNode tree, final Set<String> columns,
      final Map<String, Set<String>> tblToCols,
      final Map<String, String> exprToAlias) {
    if (tree == null) {
      return;
    }
    // Traverse the tree to get column names
    // We are doing a complete traversal so that expressions of columns
    // are also captured ex: f(cola + colb/tab1.colc)
    HQLParser.bft(tree, new ASTNodeVisitor() {
      @Override
      public void visit(TreeNode visited) {
        ASTNode node = visited.getNode();
        ASTNode parent = null;
        if (visited.getParent() != null) {
          parent = visited.getParent().getNode();
        }

        if (node.getToken().getType() == TOK_TABLE_OR_COL
            && (parent != null && parent.getToken().getType() != DOT)) {
          // Take child ident.totext
          ASTNode ident = (ASTNode) node.getChild(0);
          String column = ident.getText().toLowerCase();
          if (tblToCols != null) {
            if (exprToAlias != null && exprToAlias.values().contains(column)) {
              // column is an existing alias
              return;
            }
            Set<String> colList = tblToCols.get(DEFAULT_TABLE);
            if (colList == null) {
              colList = new HashSet<String>();
              tblToCols.put(DEFAULT_TABLE, colList);
            }
            if (!colList.contains(column)) {
              colList.add(column);
            }
          }
          columns.add(column);
        } else if (node.getToken().getType() == DOT) {
          // This is for the case where column name is prefixed by table name
          // or table alias
          // For example 'select fact.id, dim2.id ...'
          // Right child is the column name, left child.ident is table name
          ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL,
              Identifier);
          ASTNode colIdent = (ASTNode) node.getChild(1);

          String column = colIdent.getText().toLowerCase();
          String table = tabident.getText().toLowerCase();

          if (tblToCols != null) {
            Set<String> colList = tblToCols.get(table);
            if (colList == null) {
              colList = new HashSet<String>();
              tblToCols.put(table, colList);
            }
            if (!colList.contains(column)) {
              colList.add(column);
            }
          }
          columns.add(table + "." + column);
        } else if (node.getToken().getType() == TOK_SELEXPR) {
          if (exprToAlias != null) {
            // Extract column aliases for the result set, only applies to select
            // trees
            ASTNode alias = HQLParser.findNodeByPath(node, Identifier);
            if (alias != null) {
              exprToAlias.put(HQLParser.getString(node).trim(),
                  alias.getText().toLowerCase());
            }
          }
        }
      }
    });
  }

  private void extractTabAliasForCol() throws SemanticException {
    Set<String> columns = tblAliasToColumns.get(DEFAULT_TABLE);
    if (columns == null) {
      return;
    }
    for (String col : columns) {
      boolean inCube = false;
      if (cube != null) {
        Set<String> cols = cubeTabToCols.get(cube);
        if (cols.contains(col.toLowerCase())) {
          columnToTabAlias.put(col.toLowerCase(), getAliasForTabName(
              cube.getName()));
          cubeColumnsQueried.add(col);
          inCube = true;
        }
      }
      for (Dimension dim : dimensions) {
        if (cubeTabToCols.get(dim).contains(col.toLowerCase())) {
          if (!inCube) {
            String prevDim = columnToTabAlias.get(col.toLowerCase());
            if (prevDim != null && !prevDim.equals(dim.getName())) {
              throw new SemanticException(ErrorMsg.AMBIGOUS_DIM_COLUMN, col, 
                  prevDim, dim.getName());
            }
            columnToTabAlias.put(col.toLowerCase(), getAliasForTabName(
                dim.getName()));
            Set<String> dimCols = tblAliasToColumns.get(getAliasForTabName(
                dim.getName()));
            if (dimCols == null) {
              dimCols = new HashSet<String>();
              tblAliasToColumns.put(getAliasForTabName(
                dim.getName()), dimCols);
            }
            dimCols.add(col.toLowerCase());
          } else {
            // throw error because column is in both cube and dimension table
            throw new SemanticException(ErrorMsg.AMBIGOUS_CUBE_COLUMN, col, 
                cube.getName(), dim.getName());
          }
        }
      }
      if (columnToTabAlias.get(col.toLowerCase()) == null) {
        throw new SemanticException(ErrorMsg.COLUMN_NOT_FOUND, col);
      }
    }
  }

  private void doColLifeValidation() throws SemanticException {
    for (String col : cubeColumnsQueried) {
      CubeColumn column = cube.getColumnByName(col);
      for (TimeRange range : timeRanges) {
        if (column == null) {
          if (!cube.getTimedDimensions().contains(col)) {
            throw new SemanticException(ErrorMsg.NOT_A_CUBE_COLUMN);
          }
          continue;
        }
        if ((column.getStartTime() != null &&
            column.getStartTime().after(range.getFromDate())) ||
            (column.getEndTime() != null &&
            column.getEndTime().before(range.getToDate()))) {
          throw new SemanticException(ErrorMsg.NOT_AVAILABLE_IN_RANGE, col, 
            range.toString(),  (column.getStartTime() == null ? "" :
              " from:" + column.getStartTime()),
             (column.getEndTime() == null ? "" :
              " upto:" + column.getEndTime()));
        }
      }
    }
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

  public Set<Dimension> getAutoJoinDimensions() {
    return autoJoinDims;
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
      Map<Dimension, CandidateDim> dimsToQuery, AbstractCubeTable tbl, String alias) {
    if (tbl instanceof Cube) {
      return fact.getStorageString(alias);
    } else {
      return dimsToQuery.get(tbl).getStorageString(alias);
    }
  }

  String getQBFromString(CandidateFact fact,
      Map<Dimension, CandidateDim> dimsToQuery) throws SemanticException {
    String fromString = null;
    if (getJoinTree() == null) {
      if (cube != null) {
        fromString = fact.getStorageString(getAliasForTabName(cube.getName())) ;
      } else {
        assert(dimensions.size() == 1);
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
      String tblName = qb.getTabNameForAlias(joinTree.getBaseSrc()[0]).toLowerCase();
      builder.append(getStorageStringWithAlias(fact, dimsToQuery,
          cubeTbls.get(tblName), joinTree.getBaseSrc()[0]));
      if (joinTree.getJoinCond()[0].getJoinType().equals(JoinType.RIGHTOUTER)) {
        joiningTable = tblName;
      }
    }
    if (joinTree.getJoinCond() != null) {
      builder.append(getString(joinTree.getJoinCond()[0].getJoinType()));
      builder.append("JOIN ");
    }
    if (joinTree.getBaseSrc()[1] == null) {
      if (joinTree.getJoinSrc() != null) {
        getQLString(joinTree.getJoinSrc(), builder, fact, dimsToQuery);
      }
    } else { // (joinTree.getBaseSrc()[1] != null){
      String tblName = qb.getTabNameForAlias(joinTree.getBaseSrc()[1]).toLowerCase();
      builder.append(getStorageStringWithAlias(fact, dimsToQuery,
          cubeTbls.get(tblName), joinTree.getBaseSrc()[1]));
      if (joinTree.getJoinCond()[0].getJoinType().equals(JoinType.LEFTOUTER)) {
        joiningTable = tblName;
      }
    }

    String joinCond = joinConds.get(joinTree);
    if (joinCond != null) {
      builder.append(" ON ");
      builder.append(joinCond);
      if (joiningTable != null) {
        // assuming the joining table to be dimension table
        HQLContext.appendWhereClause(builder,
            dimsToQuery.get(cubeTbls.get(joiningTable)).whereClause, true);
        dimsToQuery.get(cubeTbls.get(joiningTable)).setWhereClauseAdded();
      }
    } else {
      throw new SemanticException(ErrorMsg.NO_JOIN_CONDITION_AVAIABLE);
    }
  }

  private String getString(JoinType joinType) {
    switch (joinType) {
    case INNER:
      return " INNER ";
    case LEFTOUTER:
      return " LEFT OUTER ";
    case RIGHTOUTER:
      return " RIGHT OUTER ";
    case FULLOUTER:
      return " FULL OUTER ";
    case UNIQUE:
      return " UNIQUE ";
    case LEFTSEMI:
      return " LEFT SEMI ";
    }
    return null;
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

  public Map<String, Set<String>> getTblToColumns() {
    return tblAliasToColumns;
  }

  public Set<String> getColumnsQueried(String dimName) {
    return tblAliasToColumns.get(getAliasForTabName(dimName));
  }

  public Map<String, String> getColumnsToTableAlias() {
    return columnToTabAlias;
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

  public Map<CubeQueryExpr, Set<String>> getExprToCols() {
    return exprToCols;
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
      return cubeMeasureNames.contains(col.trim().toLowerCase());
    } else {
      String cubeName = split[0].trim();
      String colName = split[1].trim();
      if (cubeName.equalsIgnoreCase(cube.getName()) ||
          cubeName.equalsIgnoreCase(getAliasForTabName(cube.getName()))) {
        return cubeMeasureNames.contains(colName.toLowerCase());
      } else {
        return false;
      }
    }
  }

  public boolean hasAggregateOnCol(String col) {
    return aggregateCols.contains(col);
  }

  public boolean isAggregateExpr(String expr) {
    return aggregateExprs.contains(expr == null ? expr : expr.toLowerCase());
  }

  public boolean hasAggregates() {
    return !aggregateExprs.isEmpty() || (cube !=null);
  }
  public String getAlias(String expr) {
    return exprToAlias.get(expr);
  }

  public Set<String> getCubeColumnsQueried() {
    return cubeColumnsQueried;
  }

  public Map<AbstractCubeTable, Set<String>> getCubeTabToCols() {
    return cubeTabToCols;
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

  public void addExprToAlias(String expr, String alias) {
    if (exprToAlias != null) {
      exprToAlias.put(expr.trim().toLowerCase(), alias);
    }
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

  public void addColumnsQueried(AbstractCubeTable table, String column) {
    String alias = getAliasForTabName(table.getName());
    Set<String> cols = tblAliasToColumns.get(alias);
    if (cols == null) {
      cols = new HashSet<String>();
      tblAliasToColumns.put(alias, cols);
    }
    cols.add(column);
  }

}
