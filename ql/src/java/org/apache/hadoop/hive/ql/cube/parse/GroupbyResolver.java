package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class GroupbyResolver implements ContextRewriter {

  public GroupbyResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    // Process Aggregations by making sure that all group by keys are projected;
    // and all projection fields are added to group by keylist;
    String groupByTree = cubeql.getGroupByTree();
    String selectTree = cubeql.getSelectTree();
    List<String> selectExprs = new ArrayList<String>();
    String[] sel = getExpressions(cubeql.getSelectAST()).toArray(new String[]{});
    for (String s : sel) {
      selectExprs.add(s.trim().toLowerCase());
    }
    List<String> groupByExprs = new ArrayList<String>();
    if (groupByTree != null) {
      String[] gby = getExpressions(cubeql.getGroupByAST()).toArray(new String[]{});
      for (String g : gby) {
        groupByExprs.add(g.trim().toLowerCase());
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

  private List<String> getExpressions(ASTNode node) {

    List<String> list = new ArrayList<String>();

    if (node == null) {
      return null;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      list.add(HQLParser.getString((ASTNode)node.getChild(i)));
    }

    return list;
  }

}
