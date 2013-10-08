package org.apache.hadoop.hive.ql.cube.parse;

import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABLE_OR_COL;

import org.antlr.runtime.CommonToken;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMeasure;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * <p>
 * Replace select and having columns with default aggregate functions on them,
 * if default aggregate is defined and if there isn't already an aggregate
 * function specified on the columns.
 * </p>
 *
 * <p>
 * Expressions which already contain aggregate sub-expressions will not be
 * changed.
 * </p>
 *
 * <p>
 * At this point it's assumed that aliases have been added to all columns.
 * </p>
 */
public class AggregateResolver implements ContextRewriter {
  public static final Log LOG = LogFactory.getLog(
      AggregateResolver.class.getName());
  public static final String DISABLE_AGGREGATE_RESOLVER = "hive.cube.disable.aggregate.resolver";
  private final Configuration conf;

  public AggregateResolver(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getCube() == null) {
      return;
    }

    if (conf.getBoolean(DISABLE_AGGREGATE_RESOLVER, false)) {
      return;
    }

    String rewritSelect = resolveClause(cubeql, cubeql.getSelectAST());
    cubeql.setSelectTree(rewritSelect);

    String rewritHaving = resolveClause(cubeql, cubeql.getHavingAST());
    if (StringUtils.isNotBlank(rewritHaving)) {
      cubeql.setHavingTree(rewritHaving);
    }
  }

  // We need to traverse the clause looking for eligible measures which can be wrapped inside
  // Aggregates
  // We have to skip any columns that are already inside an aggregate UDAF or
  // inside an arithmetic expression
  private String resolveClause(CubeQueryContext cubeql, ASTNode clause)
      throws SemanticException {

    if (clause == null) {
      return null;
    }

    for (int i = 0; i < clause.getChildCount(); i++) {
      transform(cubeql, clause, (ASTNode) clause.getChild(i), i);
    }

    return HQLParser.getString(clause);
  }

  private void transform(CubeQueryContext cubeql, ASTNode parent, ASTNode node,
      int nodePos) throws SemanticException {
    if (parent == null || node == null) {
      return;
    }
    int nodeType = node.getToken().getType();

    if (!(isAggregateAST(node))) {
      if (nodeType == HiveParser.TOK_TABLE_OR_COL ||
          nodeType == HiveParser.DOT) {
        // Leaf node
        ASTNode wrapped = wrapAggregate(cubeql, node);
        if (wrapped != node) {
          parent.setChild(nodePos, wrapped);
          // Check if this node has an alias
          ASTNode sibling = HQLParser.findNodeByPath(parent, Identifier);
          String expr;
          if (sibling != null) {
            expr = HQLParser.getString(parent);
          } else {
            expr = HQLParser.getString(wrapped);
          }
          cubeql.addAggregateExpr(expr.trim());
        }
      } else {
        // Dig deeper in non-leaf nodes
        for (int i = 0; i < node.getChildCount(); i++) {
          transform(cubeql, node, (ASTNode) node.getChild(i), i);
        }
      }
    }
  }

  static boolean isAggregateAST(ASTNode node) {
    int exprTokenType = node.getToken().getType();
    if (exprTokenType == HiveParser.TOK_FUNCTION
        || exprTokenType == HiveParser.TOK_FUNCTIONDI
        || exprTokenType == HiveParser.TOK_FUNCTIONSTAR) {
      assert (node.getChildCount() != 0);
      if (node.getChild(0).getType() == HiveParser.Identifier) {
        String functionName = BaseSemanticAnalyzer.unescapeIdentifier(
            node.getChild(0).getText());
        if (FunctionRegistry.getGenericUDAFResolver(functionName) != null) {
          return true;
        }
      }
    }
    return false;
  }

  // Wrap an aggregate function around the node if its a measure, leave it
  // unchanged otherwise
  private ASTNode wrapAggregate(CubeQueryContext cubeql, ASTNode node)
      throws SemanticException {

    String tabname = null;
    String colname;

    if (node.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
      colname = ((ASTNode) node.getChild(0)).getText();
    } else {
      // node in 'alias.column' format
      ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL,
          Identifier);
      ASTNode colIdent = (ASTNode) node.getChild(1);

      colname = colIdent.getText();
      tabname = tabident.getText();
    }

    String msrname = StringUtils.isBlank(tabname) ? colname : tabname + "."
        + colname;

    if (cubeql.isCubeMeasure(msrname)) {
      CubeMeasure measure = cubeql.getCube().getMeasureByName(colname);
      String aggregateFn = measure.getAggregate();

      if (StringUtils.isBlank(aggregateFn)) {
        throw new SemanticException("Default aggregate is not set for measure: "
            + colname);
      }
      ASTNode fnroot = new ASTNode(new CommonToken(HiveParser.TOK_FUNCTION));
      fnroot.setParent(node.getParent());

      ASTNode fnIdentNode = new ASTNode(new CommonToken(HiveParser.Identifier,
          aggregateFn));
      fnIdentNode.setParent(fnroot);
      fnroot.addChild(fnIdentNode);

      node.setParent(fnroot);
      fnroot.addChild(node);

      return fnroot;
    } else {
      return node;
    }
  }
}
