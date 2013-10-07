package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

public class GroupbyResolver implements ContextRewriter {

  private final boolean enabled;
  public GroupbyResolver(Configuration conf) {
    enabled = conf.getBoolean(CubeQueryConfUtil.ENABLE_AUTOMATIC_GROUP_BY_RESOLVER,
        CubeQueryConfUtil.DEFAULT_ENABLE_AUTOMATIC_GROUP_BY_RESOLVER);
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    if (!enabled) {
      return;
    }
    // Process Aggregations by making sure that all group by keys are projected;
    // and all projection fields are added to group by keylist;
    String groupByTree = cubeql.getGroupByTree();
    String selectTree = cubeql.getSelectTree();
    List<String> selectExprs = new ArrayList<String>();
    String[] sel = getExpressions(cubeql.getSelectAST(), cubeql).toArray(new String[]{});
    for (String s : sel) {
      selectExprs.add(s.trim());
    }
    List<String> groupByExprs = new ArrayList<String>();
    if (groupByTree != null) {
      String[] gby = getExpressions(cubeql.getGroupByAST(), cubeql).toArray(new String[]{});
      for (String g : gby) {
        groupByExprs.add(g.trim());
      }
    }
    // each selected column, if it is not a cube measure, and does not have
    // aggregation on the column, then it is added to group by columns.
    for (String expr : selectExprs) {
      if (cubeql.hasAggregates()) {
        expr = getExpressionWithoutAlias(cubeql, expr);
        if (!groupByExprs.contains(expr)) {
          if (!cubeql.isAggregateExpr(expr)) {
            String groupbyExpr = expr;
            if (groupByTree != null) {
              groupByTree += ", ";
              groupByTree += groupbyExpr;
            } else {
              groupByTree = new String();
              groupByTree += groupbyExpr;
            }
          }
        }
      }
    }
    if (groupByTree != null) {
      cubeql.setGroupByTree(groupByTree);
    }

    for (String expr : groupByExprs) {
      if (!contains(cubeql, selectExprs, expr)) {
        selectTree = expr + ", " + selectTree;
      }
    }
    cubeql.setSelectTree(selectTree);
  }

  private String getExpressionWithoutAlias(CubeQueryContext cubeql,
      String sel) {
    String alias = cubeql.getAlias(sel);
    if (alias != null) {
      sel = sel.substring(0, (sel.length() - alias.length())).trim();
    }
    return sel;
  }

  private boolean contains(CubeQueryContext cubeql, List<String> selExprs,
      String expr) {
    for (String sel : selExprs) {
      sel = getExpressionWithoutAlias(cubeql, sel);
      if (sel.equals(expr)) {
        return true;
      }
    }
    return false;
  }

  private List<String> getExpressions(ASTNode node, CubeQueryContext cubeql) {

    List<String> list = new ArrayList<String>();

    if (node == null) {
      return null;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      if (hasMeasure(child, cubeql)) {
        continue;
      }
      list.add(HQLParser.getString((ASTNode)node.getChild(i)));
    }

    return list;
  }

  boolean hasMeasure(ASTNode node, CubeQueryContext cubeql) {
    int nodeType = node.getToken().getType();
    if (nodeType == TOK_TABLE_OR_COL ||
      nodeType == DOT) {
      String colname;
      String tabname = null;

      if (node.getToken().getType() == TOK_TABLE_OR_COL) {
        colname = ((ASTNode) node.getChild(0)).getText();
      } else {
        // node in 'alias.column' format
        ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);
        ASTNode colIdent = (ASTNode) node.getChild(1);

        colname = colIdent.getText();
        tabname = tabident.getText();
      }

      String msrname = StringUtils.isBlank(tabname) ? colname : tabname + "."
        + colname;
      if (cubeql.hasCubeInQuery() && cubeql.isCubeMeasure(msrname)) {
        return true;
      }
    } else {
      for (int i = 0; i < node.getChildCount(); i++) {
        if (hasMeasure((ASTNode) node.getChild(i), cubeql)) {
          return true;
        }
      }
    }
    return false;
  }

}
