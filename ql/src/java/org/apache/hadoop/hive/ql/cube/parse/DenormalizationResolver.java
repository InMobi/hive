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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.AbstractBaseTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeColumn;
import org.apache.hadoop.hive.ql.cube.metadata.ReferencedDimAtrribute;
import org.apache.hadoop.hive.ql.cube.metadata.SchemaGraph.TableRelationship;
import org.apache.hadoop.hive.ql.cube.parse.CandidateTablePruneCause.CubeTableCause;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext.CandidateFact;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext.CandidateTable;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class DenormalizationResolver implements ContextRewriter {

  private static final Log LOG = LogFactory.getLog(DenormalizationResolver.class);

  public DenormalizationResolver(Configuration conf) {
  }

  public static class ReferencedQueriedColumn {
    ReferencedDimAtrribute col;
    AbstractBaseTable srcTable;
    
    ReferencedQueriedColumn(ReferencedDimAtrribute col, AbstractBaseTable srcTable) {
      this.col = col;
      this.srcTable = srcTable;
    }
  }

  public static class DenormalizedQueriedColumn {
    CubeColumn col;
    AbstractBaseTable queriedTable;
    TableRelationship denormalizedRelation;
    
    
    DenormalizedQueriedColumn(CubeColumn col, AbstractBaseTable queriedTable,
        TableRelationship denormalizedRelation) {
      this.col = col;
      this.queriedTable = queriedTable;
      this.denormalizedRelation = denormalizedRelation;
    }
  }

  public static class DenormalizationContext {
    // map of column name to all references
    private Map<String, Set<ReferencedQueriedColumn>> referencedCols = new HashMap<String, Set<ReferencedQueriedColumn>>();

    // candidate table name to all the references columns it needs
    private Map<CandidateTable, Set<String>> tableToRefCols = new HashMap<CandidateTable, Set<String>>();

    void addReferencedCol(String col, ReferencedQueriedColumn refer) {
      Set<ReferencedQueriedColumn> refCols = referencedCols.get(col);
      if (refCols == null) {
        refCols = new HashSet<ReferencedQueriedColumn>();
        referencedCols.put(col, refCols);
      }
      refCols.add(refer);
    }

    void addRefUsage(CandidateTable table, String col) {
      Set<String> refCols = tableToRefCols.get(table);
      if (refCols == null) {
        refCols = new HashSet<String>();
        tableToRefCols.put(table, refCols);
      }
      refCols.add(col);
    }

    Map<String, Set<ReferencedQueriedColumn>> getReferencedCols() {
      return referencedCols;
    }
  }

  /**
   * Find all de-normalized columns, if these columns are not directly available in candidate tables,
   * query will be replaced with the corresponding table reference
   * 
   * Also, if the column is denormalized and available in a parent table queried,
   * it would be replaced with parent table's column
   */
  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    DenormalizationContext denormCtx = cubeql.getDenormCtx();
    if (denormCtx == null) {
      cubeql.setDenormCtx(new DenormalizationContext());
      for (Map.Entry<String, Set<String>> entry : cubeql.getTblAlaisToColumns().entrySet()) {
        AbstractBaseTable tbl = (AbstractBaseTable)cubeql.getCubeTableForAlias(entry.getKey());
        Set<String> columns = entry.getValue();
        for (String column : columns) {
          CubeColumn col = tbl.getColumnByName(column);
          if (col instanceof ReferencedDimAtrribute) {
            // considering all referenced dimensions to be denormalized columns
            cubeql.getDenormCtx().addReferencedCol(column, new ReferencedQueriedColumn((ReferencedDimAtrribute) col, tbl));
          }
        }
      }
    } else if (!denormCtx.tableToRefCols.isEmpty()) {
      if (cubeql.getCube() != null && !cubeql.getCandidateFactTables().isEmpty())
      {
        for (Iterator<CandidateFact> i =
            cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
          CandidateFact cfact = i.next();
          if (denormCtx.tableToRefCols.containsKey(cfact)) {
            for (String refcol : denormCtx.tableToRefCols.get(cfact)) {
              if (denormCtx.getReferencedCols().get(refcol).isEmpty()) {
                LOG.info("Not considering fact table:" + cfact +
                    " as column " + refcol + " is not available");
                cubeql.addFactPruningMsgs(cfact.fact, new CandidateTablePruneCause(
                  cfact.fact.getName(), CubeTableCause.COLUMN_NOT_FOUND));
                i.remove();
              } else {
                // pick one of the references to answer the query
                // Replace picked reference in all the base trees
                // Add the join clause for the picked reference
              }
            }
          }
        }
      }
    }
  }

}
