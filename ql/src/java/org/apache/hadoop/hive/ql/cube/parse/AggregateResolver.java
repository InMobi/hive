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

  private final Configuration conf;

  public AggregateResolver(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getCube() == null) {
      return;
    }

    validateAggregates(cubeql, cubeql.getSelectAST(), false, false, false);
    validateAggregates(cubeql, cubeql.getHavingAST(), false, false, false);
    String rewritSelect = resolveClause(cubeql, cubeql.getSelectAST());
    cubeql.setSelectTree(rewritSelect);

    String rewritHaving = resolveClause(cubeql, cubeql.getHavingAST());
    if (StringUtils.isNotBlank(rewritHaving)) {
      cubeql.setHavingTree(rewritHaving);
    }
  }

  private void validateAggregates(CubeQueryContext cubeql, ASTNode node,
      boolean insideAggregate,
      boolean insideArithExpr, boolean insideNonAggrFn)
          throws SemanticException {
    if (node == null) {
      return;
    }

    int nodeType = node.getToken().getType();
    if (nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT) {
      // Found a column ref. If this is a measure, it should be inside an
      // aggregate if its part of
      // an arithmetic expression or an argument of a non-aggregate function
      String msrname = getColName(node);
      if (cubeql.isCubeMeasure(msrname) &&
          !insideAggregate
          && (insideArithExpr || insideNonAggrFn)) {
        throw new SemanticException("Not inside aggregate " + msrname);
      }
    } else if (HQLParser.isArithmeticOp(nodeType)) {
      // Allowed - sum ( msr1 * msr2 + msr3)
      // Allowed - sum(msr1) * sum(msr2) + sum(msr3)
      // Not allowed - msr1 + msr2 * msr3 <- Not inside aggregate
      // Not allowed - sum(msr1) + msr2 <- Aggregate only on one measure
      // count of measures within aggregates must be equal to count of measures
      // if both counts are equal and zero, then this node should be inside
      // aggregate
      int measuresInAggregates = countMeasuresInAggregates(cubeql, node, false);
      int measuresInTree = countMeasures(cubeql, node);

      if (measuresInAggregates == measuresInTree) {
        if (measuresInAggregates == 0 && !insideAggregate) {
          // (msr1 + msr2)
          throw new SemanticException("Invalid projection expression: arithmetic expression of measures "
              + HQLParser.getString(node));
        } else if (insideAggregate) {
          // sum(sum(msr1) + sum(msr2))
          throw new SemanticException("Invalid projection expression: aggreate over aggregates "
              + HQLParser.getString(node));
        } else {
          // Valid complex aggregate expression: sum(msr1) + sum(msr2)
          // We have to add it to aggregate set, as Hive analyzer will add parts of the expression,
          // but not the whole expression itself
          cubeql.addAggregateExpr(HQLParser.getString(node));
        }
      } else {
        throw new SemanticException("Invalid projection expression: mismatched aggregates and measures"
            + HQLParser.getString(node) + " measures in aggregates =" + measuresInAggregates
            + " total measures =" + measuresInTree);
      }
    } else {
      boolean isArithmetic = HQLParser.isArithmeticOp(nodeType);
      boolean isAggregate = isAggregateAST(node);
      boolean isNonAggrFn = nodeType == HiveParser.TOK_FUNCTION && !isAggregate;
      for (int i = 0; i < node.getChildCount(); i++) {
        validateAggregates(cubeql, (ASTNode) node.getChild(i), isAggregate,
            isArithmetic, isNonAggrFn);
      }
    }
  }

  private int countMeasures(CubeQueryContext cubeql, ASTNode node) {
    int nodeType = node.getToken().getType();
    if (nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT) {
      String msrname = getColName(node);
      if (cubeql.isCubeMeasure(msrname)) {
        return 1;
      } else {
        return 0;
      }
    } else {
      int count = 0;
      for (int i = 0; i < node.getChildCount(); i++) {
        count += countMeasures(cubeql, (ASTNode) node.getChild(i));
      }
      return count;
    }
  }

  private int countMeasuresInAggregates(CubeQueryContext cubeql, ASTNode node,
      boolean hasAggregateParent) throws SemanticException {
    int nodeType = node.getToken().getType();
    if (nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT) {
      String msrname = getColName(node);
      if (cubeql.isCubeMeasure(msrname) && hasAggregateParent) {
        return 1;
      } else {
        return 0;
      }
    } else {
      int count = 0;
      // Tell children if they are inside an aggregate function:
      // if this node is already in aggregate, then pass true, otherwise check if current node is
      // an aggregate and pass that

      boolean isCurrentNodeAggregate = isAggregateAST(node);

      if (hasAggregateParent && isCurrentNodeAggregate) {
        throw new SemanticException("Invalid projection - aggregate inside aggregate");
      }

      boolean isAggr = hasAggregateParent ? true : isCurrentNodeAggregate;

      for (int i = 0; i < node.getChildCount(); i++) {
        count += countMeasuresInAggregates(cubeql, (ASTNode) node.getChild(i),
            isAggr);
      }

      return count;
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

    if (!(isAggregateAST(node) || HQLParser.isArithmeticOp(nodeType))) {
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

  private boolean isAggregateAST(ASTNode node) {
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
    String colname = null;

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

  private String getColName(ASTNode node) {
    String tabname = null;
    String colname = null;
    int nodeType = node.getToken().getType();
    if (nodeType == HiveParser.TOK_TABLE_OR_COL) {
      colname = ((ASTNode) node.getChild(0)).getText();
    } else {
      // node in 'alias.column' format
      ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL,
          Identifier);
      ASTNode colIdent = (ASTNode) node.getChild(1);

      colname = colIdent.getText();
      tabname = tabident.getText();
    }

    return StringUtils.isBlank(tabname) ? colname : tabname + "." + colname;
  }
}
