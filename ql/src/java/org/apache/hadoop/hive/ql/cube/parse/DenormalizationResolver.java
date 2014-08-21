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
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.AbstractBaseTable;
import org.apache.hadoop.hive.ql.cube.metadata.AbstractCubeTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeColumn;
import org.apache.hadoop.hive.ql.cube.metadata.CubeInterface;
import org.apache.hadoop.hive.ql.cube.metadata.ReferencedDimAtrribute;
import org.apache.hadoop.hive.ql.cube.metadata.SchemaGraph;
import org.apache.hadoop.hive.ql.cube.metadata.SchemaGraph.TableRelationship;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext.CandidateTable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
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
    // map of column name to all denormalized columns
    private Map<String, Set<DenormalizedQueriedColumn>> denormColumns = new HashMap<String, Set<DenormalizedQueriedColumn>>();

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

    void addDenormCol(String col, DenormalizedQueriedColumn denorm) {
      Set<DenormalizedQueriedColumn> denorms = denormColumns.get(col);
      if (denorms == null) {
        denorms = new HashSet<DenormalizedQueriedColumn>();
        denormColumns.put(col, denorms);
      }
      denorms.add(denorm);
    }

    Map<String, Set<ReferencedQueriedColumn>> getReferencedCols() {
      return referencedCols;
    }

    Map<String, Set<DenormalizedQueriedColumn>> getDenormCols() {
      return denormColumns;
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
    SchemaGraph schemaGraph;
    try {
      schemaGraph = cubeql.getMetastoreClient().getSchemaGraph();
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
    for (Map.Entry<String, Set<String>> entry : cubeql.getTblAlaisToColumns().entrySet()) {
      AbstractBaseTable tbl = (AbstractBaseTable)cubeql.getCubeTableForAlias(entry.getKey());
      boolean isCube = tbl instanceof CubeInterface;
      Set<String> columns = entry.getValue();
      for (String column : columns) {
        CubeColumn col = tbl.getColumnByName(column);
        if (col instanceof ReferencedDimAtrribute) {
          // considering all referenced dimensions to be denormalized columns
          cubeql.getDenormCtx().addReferencedCol(column, new ReferencedQueriedColumn((ReferencedDimAtrribute) col, tbl));
        }
/*
        // Ex: queried column is dimx.colx which is available as cubex.dimxcolx
        // or dimx.colx which is available as dima.dimacola
        // check if the column is referenced by anyother column
        if (!isCube) {
          // skipping cube columns because wont be referenced any other tables
          if (cubeql.getCube() != null) {
            // check if column is referenced by queried cube relationship
            Map<AbstractCubeTable, Set<TableRelationship>> cgraph = 
                schemaGraph.getCubeGraph(cubeql.getCube());
            for (Map.Entry<AbstractCubeTable, Set<TableRelationship>> gentry: cgraph.entrySet()) {
              for (TableRelationship relation : gentry.getValue()) {
                if (relation.getToTable().getName().equalsIgnoreCase(tbl.getName())
                    && relation.getToColumn().equalsIgnoreCase(col.getName())) {
                  LOG.info("Found denormalized field for queried column" + column);
                  cubeql.getDenormCtx().addDenormCol(column, new DenormalizedQueriedColumn(col, tbl, relation));
                }
              }
            }
          }
          // check if column is referenced by a dimension relation
          Map<AbstractCubeTable, Set<TableRelationship>> dimgraph = 
              schemaGraph.getDimOnlyGraph();
          for (Map.Entry<AbstractCubeTable, Set<TableRelationship>> gentry: dimgraph.entrySet()) {
            for (TableRelationship relation : gentry.getValue()) {
              if (relation.getToTable().getName().equalsIgnoreCase(tbl.getName())
                  && relation.getToColumn().equalsIgnoreCase(col.getName())) {
                LOG.info("Found denormalized field for queried column" + column);
                cubeql.getDenormCtx().addDenormCol(column, new DenormalizedQueriedColumn(col, tbl, relation));
              }
            }
          }
        }
        */
      }
    }
  }

}
