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
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABLE_OR_COL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.cube.metadata.AbstractBaseTable;
import org.apache.hadoop.hive.ql.cube.metadata.DerivedCube;
import org.apache.hadoop.hive.ql.cube.metadata.Dimension;
import org.apache.hadoop.hive.ql.cube.metadata.ExprColumn;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser.ASTNodeVisitor;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser.TreeNode;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class ExpressionResolver implements ContextRewriter {

  public ExpressionResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    resolveClause(cubeql, cubeql.getSelectAST());
    resolveClause(cubeql, cubeql.getWhereAST());
    resolveClause(cubeql, cubeql.getGroupByAST());
    resolveClause(cubeql, cubeql.getHavingAST());
    resolveClause(cubeql, cubeql.getOrderByAST());
  }

  private void resolveClause(final CubeQueryContext cubeql, ASTNode clause) throws SemanticException {
    if (clause == null) {
      return;
    }

    // Traverse the tree and resolve expression columns
    HQLParser.bft(clause, new ASTNodeVisitor() {
      @Override
      public void visit(TreeNode visited) throws SemanticException {
        ASTNode node = visited.getNode();
        int childcount = node.getChildCount();
        for (int i = 0; i < childcount; i++) {
          ASTNode current = (ASTNode)node.getChild(i);
          if (current.getToken().getType() == TOK_TABLE_OR_COL
              && (node != null && node.getToken().getType() != DOT)) {
            // Take child ident.totext
            ASTNode ident = (ASTNode) current.getChild(0);
            String column = ident.getText().toLowerCase();
            ASTNode childExpr = getExprAST(cubeql, column);
            if (childExpr != null) {
              node.setChild(i, HQLParser.copyAST(childExpr));
            }
          } else if (current.getToken().getType() == DOT) {
            // This is for the case where column name is prefixed by table name
            // or table alias
            // For example 'select fact.id, dim2.id ...'
            // Right child is the column name, left child.ident is table name
            ASTNode tabident = HQLParser.findNodeByPath(current, TOK_TABLE_OR_COL,
                Identifier);
            ASTNode colIdent = (ASTNode) current.getChild(1);

            String column = colIdent.getText().toLowerCase();

            ASTNode childExpr = getExprAST(cubeql, tabident.getText().toLowerCase(), column);
            if (childExpr != null) {
              node.setChild(i, HQLParser.copyAST(childExpr));
            }
          }
        }
      }

    });
  }

  private ASTNode getExprAST(final CubeQueryContext cubeql, String table,
      String column) throws SemanticException {
    if (cubeql.getQueriedTable(table) == null) {
      cubeql.addQueriedTable(table);
    }
    if (!(cubeql.getQueriedTable(table) instanceof AbstractBaseTable)) {
      return null;
    }
    if (((AbstractBaseTable)cubeql.getQueriedTable(table)).getExpressionByName(column) == null) {
      return null;
    }
    try {
      return ((AbstractBaseTable)cubeql.getQueriedTable(table)).getExpressionByName(column).getAst();
    } catch (ParseException e) {
      throw new SemanticException(e);
    }
  }

  private ASTNode getExprAST(final CubeQueryContext cubeql, final String column) throws SemanticException {
    ExprColumn expr = null;
    AbstractBaseTable table = null;
    if (cubeql.getCube() != null && !(cubeql.getCube() instanceof DerivedCube)) {
      // no expression resolver for derived cubes
      if (cubeql.getCube().getExpressionNames().contains(column.toLowerCase())) {
        expr = cubeql.getCube().getExpressionByName(column);
        table = (AbstractBaseTable)cubeql.getCube();
      }
    }
    if (cubeql.getDimensions() != null) {
      for (Dimension dim : cubeql.getDimensions()) {
        if (dim.getExpressionNames().contains(column.toLowerCase())) {
          if (expr != null) {
            throw new SemanticException(ErrorMsg.AMBIGOUS_DIM_COLUMN, table.getName(), dim.getName());
          }
          expr = dim.getExpressionByName(column);
          table = dim;
        }
      }
    }
    if (expr == null) {
      return null;
    }
    try {
      return expr.getAst();
    } catch (ParseException e) {
      throw new SemanticException(e);
    }
  }

  /*  // Fully resolve the expression by resolving all the children, if they expressions by themselves
  private ASTNode getExprAST(final CubeQueryContext cubeql, 
      final AbstractBaseTable table, final ExprColumn expr)
          throws ParseException, SemanticException {
    // Traverse the tree and resolve expression columns
    HQLParser.bft(expr.getAst(), new ASTNodeVisitor() {
      @Override
      public void visit(TreeNode visited) throws SemanticException {
        ASTNode node = visited.getNode();

        int childcount = node.getChildCount();
        for (int i = 0; i < childcount; i++) {
          ASTNode current = (ASTNode)node.getChild(i);
          if (current.getToken().getType() == TOK_TABLE_OR_COL
              && (node != null && node.getToken().getType() != DOT)) {
            // Take child ident.totext
            ASTNode ident = (ASTNode) current.getChild(0);
            String column = ident.getText().toLowerCase();
            ASTNode childExpr = getExprAST(cubeql, table, column);
            if (childExpr != null) {
              node.setChild(i, childExpr);
            }
          } else if (current.getToken().getType() == DOT) {
            // This is for the case where column name is prefixed by table name
            // or table alias
            // For example 'select fact.id, dim2.id ...'
            // Right child is the column name, left child.ident is table name
            ASTNode tabident = HQLParser.findNodeByPath(current, TOK_TABLE_OR_COL,
                Identifier);
            ASTNode colIdent = (ASTNode) current.getChild(1);

            String column = colIdent.getText().toLowerCase();

            ASTNode childExpr = getExprAST(cubeql, tabident.getText().toLowerCase(), column);
            if (childExpr != null) {
              node.setChild(i, childExpr);
            }
          }
        }
      }
    });

    return expr.getAst();
  }
   */
}
