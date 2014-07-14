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
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FUNCTIONSTAR;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_SELEXPR;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABLE_OR_COL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser.ASTNodeVisitor;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser.TreeNode;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class ColumnResolver implements ContextRewriter {

  public ColumnResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    extractColumns(cubeql);
  }

  private void extractColumns(CubeQueryContext cubeql) throws SemanticException {
    // Check if its 'select * from...'
    ASTNode selTree = cubeql.getSelectAST();
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
    getColsForTree(cubeql, cubeql.getSelectAST());
    getColsForTree(cubeql, cubeql.getWhereAST());
    getColsForTree(cubeql, cubeql.getJoinTree());
    getColsForTree(cubeql, cubeql.getGroupByAST());
    getColsForTree(cubeql, cubeql.getHavingAST());
    getColsForTree(cubeql, cubeql.getOrderByAST());

    // Update join dimension tables
    for (String table : cubeql.getTblAlaisToColumns().keySet()) {
      try {
        if (!CubeQueryContext.DEFAULT_TABLE.equalsIgnoreCase(table)) {
          cubeql.addQueriedTable(table);
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      }
    }

  }

  private void getColsForTree(final CubeQueryContext cubeql, ASTNode tree) throws SemanticException {
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
          if (cubeql.getExprToAliasMap().values().contains(column)) {
            // column is an existing alias
            return;
          }
          cubeql.addColumnsQueried(CubeQueryContext.DEFAULT_TABLE, column);
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

          cubeql.addColumnsQueried(table, column);
        } else if (node.getToken().getType() == TOK_SELEXPR) {
          // Extract column aliases for the result set, only applies to select
          // trees
          ASTNode alias = HQLParser.findNodeByPath(node, Identifier);
          if (alias != null) {
            cubeql.addExprToAlias(node, alias);
          }
        }
      }
    });
  }
}
