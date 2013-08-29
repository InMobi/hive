package org.apache.hadoop.hive.ql.cube.parse;

import static org.apache.hadoop.hive.ql.parse.HiveParser.DOT;
import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_ALLCOLREF;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FUNCTION;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FUNCTIONSTAR;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_SELEXPR;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABLE_OR_COL;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TMP_FILE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.hadoop.hive.ql.cube.metadata.AbstractCubeTable;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.MetastoreUtil;
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
  private Cube cube;
  // All measures in this cube
  private List<String> cubeMeasureNames;
  // All dimensions in this cube
  private List<String> cubeDimNames;
  // Dimensions used to decide partitions
  private Set<String> timedDimensions;
  // Dimension tables accessed in the query
  protected Set<CubeDimensionTable> dimensions =
      new HashSet<CubeDimensionTable>();

  // Dimension table accessed but not in from
  protected Set<CubeDimensionTable> autoJoinDims = new HashSet<CubeDimensionTable>();

  // Name to table object mapping of tables accessed in this query
  private final Map<String, AbstractCubeTable> cubeTbls =
      new HashMap<String, AbstractCubeTable>();
  // Mapping of table objects to all columns of that table accessed in the query
  private final Map<AbstractCubeTable, List<String>> cubeTabToCols =
      new HashMap<AbstractCubeTable, List<String>>();

  // Alias name to fields queried
  private final Map<String, List<String>> tblAliasToColumns =
      new HashMap<String, List<String>>();
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
  // Map from fact to number of partitions
  private final Map<CubeDimensionTable, List<String>> dimStorageMap =
      new HashMap<CubeDimensionTable, List<String>>();
  private final Map<String, String> storageTableToWhereClause =
      new HashMap<String, String>();
  private final Map<AbstractCubeTable, Set<String>> storageTableToQuery =
      new HashMap<AbstractCubeTable, Set<String>>();

  // query trees
  private String whereTree;
  private String havingTree;
  private String orderByTree;
  private String selectTree;
  private String groupByTree;
  private final ASTNode joinTree;
  private ASTNode havingAST;
  private ASTNode selectAST;
  private ASTNode whereAST;
  private ASTNode orderByAST;
  private ASTNode groupByAST;
  private CubeMetastoreClient client;
  private final boolean qlEnabledMultiTableSelect;

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
    this.joinTree = qb.getParseInfo().getJoinExpr();
    // read conf
    qlEnabledMultiTableSelect = conf.getBoolean(
        CubeQueryConfUtil.ENABLE_MULTI_TABLE_SELECT,
        CubeQueryConfUtil.DEFAULT_MULTI_TABLE_SELECT);

    extractMetaTables();
    extractTimeRange();
    extractColumns();
    extractTabAliasForCol();
    findCandidateFactTables();
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
              throw new SemanticException("More than one cube accessed in" +
                  " query");
            }
          }
          cube = client.getCube(tblName);
          cubeMeasureNames = MetastoreUtil.getCubeMeasureNames(cube);
          cubeDimNames = MetastoreUtil.getCubeDimensionNames(cube);
          timedDimensions = cube.getTimedDimensions();
          List<String> cubeCols = new ArrayList<String>();
          cubeCols.addAll(cubeMeasureNames);
          cubeCols.addAll(cubeDimNames);
          if (timedDimensions != null) {
            cubeCols.addAll(timedDimensions);
          }
          cubeTabToCols.put(cube, cubeCols);
          cubeTbls.put(tblName.toLowerCase(), cube);
        } else if (client.isDimensionTable(tblName)) {
          CubeDimensionTable dim = client.getDimensionTable(tblName);
          dimensions.add(dim);
          cubeTabToCols.put(dim, MetastoreUtil.getColumnNames(dim));
          cubeTbls.put(tblName.toLowerCase(), dim);
        }
      }
      if (cube == null && dimensions.size() == 0) {
        throw new SemanticException("Neither cube nor dimensions accessed");
      }
      if (cube != null) {
        for (CubeFactTable fact : client.getAllFactTables(cube)) {
          CandidateFact cfact = new CandidateFact(fact);
          cfact.enabledMultiTableSelect = qlEnabledMultiTableSelect;
          candidateFacts.add(cfact);
          cubeTabToCols.put(fact, MetastoreUtil.getColumnNames(fact));
        }
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
      throw new SemanticException("No filter specified");
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
    int startPos = timenode.getCharPositionInLine();
    builder.astNode(timenode);

    String timeDimName = PlanUtils.stripQuotes(timenode.getChild(1).getText());
    if (cube.getTimedDimensions().contains(timeDimName)) {
      builder.partitionColumn(timeDimName);
    } else {
      throw new SemanticException(timeDimName + " is not a time dimension");
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
    try {
      builder.fromDate(DateUtil.resolveDate(fromDateRaw, now));
      if (StringUtils.isNotBlank(toDateRaw)) {
        builder.toDate(DateUtil.resolveDate(toDateRaw, now));
      } else {
        builder.toDate(now);
      }
    } catch (HiveException e) {
      throw new SemanticException(e);
    }

    TimeRange range = builder.build();
    range.validate();

    if (parent != null) {
      parent.setChild(range);
    } else {
      timeRanges.add(range);
    }

    // check if last argument is another time range function
    ASTNode lastChild = (ASTNode) timenode.getChild(timenode.getChildCount() - 1);
    if (lastChild.getToken().getType() == TOK_FUNCTION) {
      ASTNode fname = HQLParser.findNodeByPath(lastChild, Identifier);
      if (fname != null && TIME_RANGE_FUNC.equalsIgnoreCase(fname.getText())) {
        processTimeRangeFunction(lastChild, range);
      }
    }
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
          throw new SemanticException("Selecting allColumns is not yet " +
              "supported");
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
          if (client.isDimensionTable(table)) {
            autoJoinDims.add(client.getDimensionTable(table));
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
      final Map<String, List<String>> tblToCols,
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
            List<String> colList = tblToCols.get(DEFAULT_TABLE);
            if (colList == null) {
              colList = new ArrayList<String>();
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
            List<String> colList = tblToCols.get(table);
            if (colList == null) {
              colList = new ArrayList<String>();
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
    List<String> columns = tblAliasToColumns.get(DEFAULT_TABLE);
    if (columns == null) {
      return;
    }
    for (String col : columns) {
      boolean inCube = false;
      if (cube != null) {
        List<String> cols = cubeTabToCols.get(cube);
        if (cols.contains(col.toLowerCase())) {
          columnToTabAlias.put(col.toLowerCase(), getAliasForTabName(
              cube.getName()));
          cubeColumnsQueried.add(col);
          inCube = true;
        }
      }
      for (CubeDimensionTable dim : dimensions) {
        if (cubeTabToCols.get(dim).contains(col.toLowerCase())) {
          if (!inCube) {
            String prevDim = columnToTabAlias.get(col.toLowerCase());
            if (prevDim != null && !prevDim.equals(dim.getName())) {
              throw new SemanticException("Ambiguous column:" + col
                  + " in dimensions '" + prevDim + "' and '"
                  + dim.getName() + "'");
            }
            columnToTabAlias.put(col.toLowerCase(), getAliasForTabName(
                dim.getName()));
          } else {
            // throw error because column is in both cube and dimension table
            throw new SemanticException("Ambiguous column:" + col
                + " in cube: " + cube.getName() + " and dimension: "
                + dim.getName());
          }
        }
      }
      if (columnToTabAlias.get(col.toLowerCase()) == null) {
        throw new SemanticException("Could not find the table containing" +
            " column:" + col);
      }
    }
  }

  private void findCandidateFactTables() throws SemanticException {
    if (cube != null) {
      // go over the columns accessed in the query and find out which tables
      // can answer the query
      String str = conf.get(CubeQueryConfUtil.getValidFactTablesKey(
          cube.getName()));
      List<String> validFactTables = StringUtils.isBlank(str) ? null :
        Arrays.asList(StringUtils.split(str.toLowerCase(), ","));
      for (Iterator<CandidateFact> i = candidateFacts.iterator();
          i.hasNext();) {
        CubeFactTable fact = i.next().fact;
        if (validFactTables != null) {
          if (!validFactTables.contains(fact.getName().toLowerCase())) {
            LOG.info("Not considering the fact table:" + fact + " as it is" +
                " not a valid fact");
            i.remove();
            continue;
          }
        }

        List<String> factCols = cubeTabToCols.get(fact);
        List<String> validFactCols = fact.getValidColumns();

        for (String col : cubeColumnsQueried) {
          if (!factCols.contains(col.toLowerCase())) {
            LOG.info("Not considering the fact table:" + fact +
                " as column " + col + " is not available");
            i.remove();
            break;
          } else {
            if (validFactCols != null) {
              if (!validFactCols.contains(col.toLowerCase())) {
                LOG.info("Not considering the fact table:" + fact +
                    " as column " + col + " is not valid");
                i.remove();
                break;
              }
            }
          }
        }
      }
      if (candidateFacts.size() == 0) {
        throw new SemanticException("No candidate fact table available to" +
            " answer the query");
      }
    }
  }


  public Cube getCube() {
    return cube;
  }

  public QB getQB() {
    return qb;
  }

  public Set<CandidateFact> getCandidateFactTables() {
    return candidateFacts;
  }

  public Set<CubeDimensionTable> getDimensionTables() {
    return dimensions;
  }

  public Set<CubeDimensionTable> getAutoJoinDimensions() {
    return autoJoinDims;
  }

  public String getAliasForTabName(String tabName) {
    for (String alias : qb.getTabAliases()) {
      if (qb.getTabNameForAlias(alias).equalsIgnoreCase(tabName)) {
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
    return joinTree;
  }

  public String getOrderByTree() {
    return orderByTree;
  }

  public Integer getLimitValue() {
    return qb.getParseInfo().getDestLimit(getClause());
  }

  private final String baseQueryFormat = "SELECT %s FROM %s";

  String getQueryFormat() {
    StringBuilder queryFormat = new StringBuilder();
    queryFormat.append(baseQueryFormat);
    if (getWhereTree() != null || hasPartitions()) {
      queryFormat.append(" WHERE %s");
    }
    if (getGroupByTree() != null) {
      queryFormat.append(" GROUP BY %s");
    }
    if (getHavingTree() != null) {
      queryFormat.append(" HAVING %s");
    }
    if (getOrderByTree() != null) {
      queryFormat.append(" ORDER BY %s");
    }
    if (getLimitValue() != null) {
      queryFormat.append(" LIMIT %s");
    }
    return queryFormat.toString();
  }

  private final String unionQueryFormat = "SELECT * FROM %s";
  String getUnionQueryFormat() {
    StringBuilder queryFormat = new StringBuilder();
    queryFormat.append(unionQueryFormat);
    if (getGroupByTree() != null) {
      queryFormat.append(" GROUP BY %s");
    }
    if (getHavingTree() != null) {
      queryFormat.append(" HAVING %s");
    }
    if (getOrderByTree() != null) {
      queryFormat.append(" ORDER BY %s");
    }
    if (getLimitValue() != null) {
      queryFormat.append(" LIMIT %s");
    }
    return queryFormat.toString();
  }

  private Object[] getQueryTreeStrings(Set<String> factStorageTables)
      throws SemanticException {
    List<String> qstrs = new ArrayList<String>();
    qstrs.add(getSelectTree());
    qstrs.add(getFromString());
    String whereString = genWhereClauseWithPartitions(factStorageTables);
    if (whereString != null) {
      qstrs.add(whereString);
    }
    if (getGroupByTree() != null) {
      qstrs.add(getGroupByTree());
    }
    if (getHavingTree() != null) {
      qstrs.add(getHavingTree());
    }
    if (getOrderByTree() != null) {
      qstrs.add(getOrderByTree());
    }
    if (getLimitValue() != null) {
      qstrs.add(String.valueOf(getLimitValue()));
    }
    return qstrs.toArray(new String[0]);
  }

  private String getStorageString(AbstractCubeTable tbl) {
    return StringUtils.join(storageTableToQuery.get(tbl), ",") + " " +
        getAliasForTabName(tbl.getName());
  }

  private String getFromString() throws SemanticException {
    String fromString = null;
    if (joinTree == null) {
      if (cube != null) {
        fromString = getStorageString(cube) ;
      } else {
        CubeDimensionTable dim = dimensions.iterator().next();
        fromString = getStorageString(dim);
      }

      if (joinsResolvedAutomatically()) {
        fromString += " " + getAutoResolvedJoinChain();
      }

    } else {
      StringBuilder builder = new StringBuilder();
      getQLString(qb.getQbJoinTree(), builder);
      fromString = builder.toString();
    }
    return fromString;
  }

  private final Set<String> tablesAlreadyAdded = new HashSet<String>();
  private boolean joinsResolvedAutomatically;
  private String autoResolvedJoinClause;

  private void getQLString(QBJoinTree joinTree, StringBuilder builder)
      throws SemanticException {
    String joiningTable = null;
    if (joinTree.getBaseSrc()[0] == null) {
      if (joinTree.getJoinSrc() != null) {
        getQLString(joinTree.getJoinSrc(), builder);
      }
    } else { // (joinTree.getBaseSrc()[0] != null){
      String tblName = qb.getTabNameForAlias(joinTree.getBaseSrc()[0]).toLowerCase();
      builder.append(getStorageString(cubeTbls.get(tblName)));
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
        getQLString(joinTree.getJoinSrc(), builder);
      }
    } else { // (joinTree.getBaseSrc()[1] != null){
      String tblName = qb.getTabNameForAlias(joinTree.getBaseSrc()[1]).toLowerCase();
      builder.append(getStorageString(cubeTbls.get(tblName)));
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
        appendWhereClause((CubeDimensionTable)cubeTbls.get(joiningTable),
            builder, true);
        tablesAlreadyAdded.add(joiningTable);
      }
    } else {
      throw new SemanticException("No join condition available");
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

  private String toHQL(Set<String> tableNames) throws SemanticException {
    findDimStorageTables();
    String qfmt = getQueryFormat();
    LOG.info("qfmt:" + qfmt);
    String baseQuery = String.format(qfmt, getQueryTreeStrings(tableNames));
    String insertString = "";
    ASTNode destTree = qb.getParseInfo().getDestForClause(clauseName);
    if (destTree != null && ((ASTNode)(destTree.getChild(0)))
        .getToken().getType() != TOK_TMP_FILE) {
      insertString = "INSERT OVERWRITE" + HQLParser.getString(
          qb.getParseInfo().getDestForClause(clauseName));
    }
    return insertString + baseQuery;
  }

  void setNonexistingParts(List<String> nonExistingParts) {
    conf.set(CubeQueryConfUtil.NON_EXISTING_PARTITIONS,
        StringUtils.join(nonExistingParts, ','));
  }

  private String unionQuery() {
    // TODO Auto-generated method stub
    return null;
  }

  private void appendWhereClause(StringBuilder whereWithoutTimerange,
      String whereClause, boolean hasMore) {
    if (hasMore) {
      whereWithoutTimerange.append(" AND ");
    }
    appendWhereClause(whereWithoutTimerange, whereClause);
  }

  private void appendWhereClause(CubeDimensionTable dim,
      StringBuilder whereString,
      boolean hasMore) {
    if (tablesAlreadyAdded.contains(dim.getName())) {
      LOG.debug("Skipping already added where clause for " + dim);
      return;
    }
    String whereClause = storageTableToWhereClause.get(
        storageTableToQuery.get(dim).iterator().next());
    if (whereClause != null) {
      appendWhereClause(whereString, whereClause, hasMore);
    }
  }

  private void appendWhereClause(StringBuilder whereWithoutTimerange,
      String whereClause) {
    whereWithoutTimerange.append("(");
    whereWithoutTimerange.append(whereClause);
    whereWithoutTimerange.append(")");
  }

  private String genWhereClauseWithPartitions(Set<String> factStorageTables) {
    String originalWhere = getWhereTree();
    StringBuilder whereBuf;

    if (factStorageTables != null) {
      // Get Rid of the time range.
      int timeRangeBegin = originalWhere.indexOf(TIME_RANGE_FUNC);
      int timeRangeEnd = originalWhere.indexOf(')', timeRangeBegin) + 1;

      whereBuf = new StringBuilder(originalWhere.substring(0, timeRangeBegin));
      whereBuf.append("(");
      Iterator<String> it = factStorageTables.iterator();
      while (it.hasNext()) {
        whereBuf.append(storageTableToWhereClause.get(it.next()));
        if (it.hasNext()) {
          whereBuf.append(" OR ");
        }
      }
      whereBuf.append(")")
      .append(originalWhere.substring(timeRangeEnd));
    } else {
      if (originalWhere != null) {
        whereBuf = new StringBuilder(originalWhere);
      } else {
        whereBuf = new StringBuilder();
      }
    }

    // add where clause for all dimensions
    Iterator<CubeDimensionTable> it = dimensions.iterator();
    if (it.hasNext()) {
      CubeDimensionTable dim = it.next();
      appendWhereClause(dim, whereBuf, factStorageTables != null);
      while (it.hasNext()) {
        dim = it.next();
        appendWhereClause(dim, whereBuf, true);
      }
    }
    if (whereBuf.length() == 0) {
      return null;
    }
    return whereBuf.toString();
  }

  private void findDimStorageTables() {
    Iterator<CubeDimensionTable> it = dimensions.iterator();
    while (it.hasNext()) {
      CubeDimensionTable dim = it.next();
      String storageTable = dimStorageMap.get(dim).get(0);
      storageTableToQuery.put(dim, Collections.singleton(storageTable));
    }
  }

  public String toHQL() throws SemanticException {
    CandidateFact fact = null;
    if (hasCubeInQuery()) {
      if (candidateFacts.size() > 0) {
        fact = candidateFacts.iterator().next();
        LOG.info("Available candidate facts:" + candidateFacts +
            ", picking up " + fact.fact + " for querying");
      }
    }
    if (fact == null && !hasDimensionInQuery()) {
      throw new SemanticException("No valid fact table available");
    }

    if (fact != null) {
      if (fact.storageTables.isEmpty()) {
        throw new SemanticException("No storage table available for candidate"
            + " fact:" + fact);
      }
      // choosing the first storage table one in the list
      storageTableToQuery.put(getCube(), fact.storageTables);
      if (fact.storageTables != null && fact.storageTables.size() > 1
          && !fact.enabledMultiTableSelect) {
        return unionQuery();
      }
      return toHQL(fact.storageTables);
    } else {
      return toHQL(null);
    }
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

  public void setDimStorageMap(
      Map<CubeDimensionTable, List<String>> dimStorageMap) {
    this.dimStorageMap.putAll(dimStorageMap);
  }

  public Map<CubeDimensionTable, List<String>> getDimStorageMap() {
    return this.dimStorageMap;
  }

  public Map<String, String> getStorageTableToWhereClause() {
    return storageTableToWhereClause;
  }

  public void setStorageTableToWhereClause(Map<String, String> whereClauseMap) {
    storageTableToWhereClause.putAll(whereClauseMap);
  }

  public boolean hasPartitions() {
    return !storageTableToWhereClause.isEmpty();
  }

  public Map<String, List<String>> getTblToColumns() {
    return tblAliasToColumns;
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

  public Map<AbstractCubeTable, List<String>> getCubeTabToCols() {
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

  public CubeMetastoreClient getMetastoreClient() {
    return client;
  }

  public void addExprToAlias(String expr, String alias) {
    if (exprToAlias != null) {
      exprToAlias.put(expr.trim().toLowerCase(), alias);
    }
  }

  public void setJoinsResolvedAutomatically(boolean flag) {
    this.joinsResolvedAutomatically = flag;
  }

  public boolean joinsResolvedAutomatically() {
    return joinsResolvedAutomatically;
  }

  public void setAutoResolvedJoinClause(String clause) {
    this.autoResolvedJoinClause = clause;
  }

  public String getAutoResolvedJoinChain() {
    return this.autoResolvedJoinClause;
  }

  public List<TimeRange> getTimeRanges() {
    return timeRanges;
  }

  static class CandidateFact {
    CubeFactTable fact;
    Set<String> storageTables;
    boolean enabledMultiTableSelect;
    int numQueriedParts;
    CandidateFact(CubeFactTable fact) {
      this.fact = fact;
    }
    
    public String toString() {
      return fact.toString();
    }
  }

}
