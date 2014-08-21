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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.cube.metadata.AbstractCubeTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.Dimension;
import org.apache.hadoop.hive.ql.cube.parse.CandidateTablePruneCause.CubeTableCause;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext.CandidateDim;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext.CandidateFact;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class CandidateTableResolver implements ContextRewriter {

  private static Log LOG = LogFactory.getLog(CandidateTableResolver.class.getName());
  private Map<AbstractCubeTable, Set<String>> cubeTabToCols;
  private boolean qlEnabledMultiTableSelect;

  public CandidateTableResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    cubeTabToCols = new HashMap<AbstractCubeTable, Set<String>>();
    qlEnabledMultiTableSelect = cubeql.getHiveConf().getBoolean(
        CubeQueryConfUtil.ENABLE_MULTI_TABLE_SELECT,
        CubeQueryConfUtil.DEFAULT_MULTI_TABLE_SELECT); 
    populateCandidateTables(cubeql);
    resolveCandidateFactTables(cubeql);
    resolveCandidateDimTables(cubeql);
  }

  private void populateCandidateTables(CubeQueryContext cubeql) throws SemanticException {
    try {
    if (cubeql.getCube() != null) {
      List<CubeFactTable> factTables = cubeql.getMetastoreClient().getAllFactTables(cubeql.getCube());
      if (factTables.isEmpty()) {
        throw new SemanticException(ErrorMsg.NO_CANDIDATE_FACT_AVAILABLE,
            cubeql.getCube().getName() + " does not have any facts");
      }
      for (CubeFactTable fact : factTables) {
        CandidateFact cfact = new CandidateFact(fact);
        cfact.enabledMultiTableSelect = qlEnabledMultiTableSelect;
        cubeql.getCandidateFactTables().add(cfact);
        cubeTabToCols.put(fact, fact.getAllFieldNames());
      }
    }
    
    if (cubeql.getDimensions().size() != 0) {
      for (Dimension dim : cubeql.getDimensions()) {
        Set<CandidateDim> candidates = new HashSet<CandidateDim>();
        cubeql.getCandidateDimTables().put(dim, candidates);
        List<CubeDimensionTable> dimtables = cubeql.getMetastoreClient().getAllDimensionTables(dim);
        if (dimtables.isEmpty()) {
          throw new SemanticException(ErrorMsg.NO_CANDIDATE_DIM_AVAILABLE,
              dim.getName(), "Dimension tables do not exist");
        }
        for (CubeDimensionTable dimtable : dimtables) {
          CandidateDim cdim = new CandidateDim(dimtable);
          candidates.add(cdim);
          cubeTabToCols.put(dimtable, dimtable.getAllFieldNames());
        }
      }
    }    
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  void resolveCandidateFactTables(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getCube() != null) {
      String str = cubeql.getHiveConf().get(CubeQueryConfUtil.getValidFactTablesKey(
          cubeql.getCube().getName()));
      List<String> validFactTables = StringUtils.isBlank(str) ? null :
        Arrays.asList(StringUtils.split(str.toLowerCase(), ","));
      Set<String> cubeColsQueried = cubeql.getColumnsQueried(cubeql.getCube().getName());

      // Remove fact tables based on columns in the query
      for (Iterator<CandidateFact> i = cubeql.getCandidateFactTables().iterator();
          i.hasNext();) {
        boolean isRemoved = false;
        CandidateFact cfact = i.next();
        CubeFactTable fact = cfact.fact;

        if (validFactTables != null) {
          if (!validFactTables.contains(fact.getName().toLowerCase())) {
            LOG.info("Not considering fact table:" + fact + " as it is" +
                " not a valid fact");
            cubeql.addFactPruningMsgs(fact,
                new CandidateTablePruneCause(fact.getName(), CubeTableCause.INVALID));
            i.remove();
            continue;
          }
        }

        // go over the columns accessed in the query and find out which tables
        // can answer the query
        Set<String> factCols = cubeTabToCols.get(fact);
        List<String> validFactCols = fact.getValidColumns();

        for (String col : cubeColsQueried) {
          if (!cubeql.getCube().getTimedDimensions().contains(col.toLowerCase())) {
            if (validFactCols != null) {
              if (!validFactCols.contains(col.toLowerCase())) {
                if (cubeql.getDenormCtx().getReferencedCols().containsKey(col)) {
                  // available as referenced col
                  // check if the reference source is reachable?
                  cubeql.getDenormCtx().addRefUsage(cfact, col);
                } else {
                LOG.info("Not considering fact table:" + fact +
                    " as column " + col + " is not valid");
                cubeql.addFactPruningMsgs(fact, new CandidateTablePruneCause(
                    fact.getName(), CubeTableCause.COLUMN_NOT_VALID));
                i.remove();
                isRemoved = true;
                break;
                }
              }
            } else if(!factCols.contains(col.toLowerCase())) {
              if (cubeql.getDenormCtx().getReferencedCols().containsKey(col)) {
                // available as referenced col
                // check if the reference source is reachable?
                cubeql.getDenormCtx().addRefUsage(cfact, col);
              } else {
              LOG.info("Not considering fact table:" + fact +
                  " as column " + col + " is not available");
              cubeql.addFactPruningMsgs(fact, new CandidateTablePruneCause(
                fact.getName(), CubeTableCause.COLUMN_NOT_FOUND));
              isRemoved = true;
              i.remove();
              break;
              }
            }
          }
        }

        // Check if the candidate fact has at least one column in any of the join paths
        if (!isRemoved && !filterJoinPathTables(cubeql, (AbstractCubeTable) cubeql.getCube(), factCols, validFactCols)) {
          i.remove();
          LOG.info("Not considering fact table:" + fact + " as it does not have columns in any of the join paths");
          cubeql.addFactPruningMsgs(fact,
            new CandidateTablePruneCause(fact.getName(), CubeTableCause.NO_COLUMN_PART_OF_A_JOIN_PATH));
        }

      }
      if (cubeql.getCandidateFactTables().size() == 0) {
        throw new SemanticException(ErrorMsg.NO_FACT_HAS_COLUMN,
            cubeColsQueried.toString());
      }
    }
  }

  void resolveCandidateDimTables(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getDimensions().size() != 0) {
      for (Dimension dim : cubeql.getDimensions()) {
        // go over the columns accessed in the query and find out which tables
        // can answer the query
        for (Iterator<CandidateDim> i = cubeql.getCandidateDimTables().get(dim).iterator();
            i.hasNext();) {
          CubeDimensionTable dimtable = i.next().dimtable;
          boolean isRemoved = false;

          Set<String> dimCols = cubeTabToCols.get(dimtable);

          if (cubeql.getColumnsQueried(dim.getName()) != null) {
            for (String col : cubeql.getColumnsQueried(dim.getName())) {
              if (!dimCols.contains(col.toLowerCase())) {
                LOG.info("Not considering dimtable:" + dimtable +
                  " as column " + col + " is not available");
                cubeql.addDimPruningMsgs(dim, dimtable, new CandidateTablePruneCause(
                  dimtable.getName(), CubeTableCause.COLUMN_NOT_FOUND));
                i.remove();
                isRemoved = true;
                break;
              }
            }
          }

          if (!isRemoved && !filterJoinPathTables(cubeql, dim, dimCols, null)) {
            i.remove();
            LOG.info("Not considering dimtable:" + dimtable +" as its columns are not part of any join paths");
            cubeql.addDimPruningMsgs(dim, dimtable, new CandidateTablePruneCause(
              dimtable.getName(), CubeTableCause.NO_COLUMN_PART_OF_A_JOIN_PATH));
          }
        }

        if (cubeql.getCandidateDimTables().get(dim).size() == 0) {
          throw new SemanticException(ErrorMsg.NO_DIM_HAS_COLUMN,
              dim.getName(), cubeql.getColumnsQueried(dim.getName()).toString());
        }
      }
    }
  }


  /**
   * @return true if at least one of the columns of this table is present in any of the join paths
   */
  protected boolean filterJoinPathTables(CubeQueryContext context,
                                         AbstractCubeTable table,
                                         Set<String> columnsOfTable,
                                         List<String> validColumns) {
    JoinResolver.AutoJoinContext joinContext = context.getAutoJoinCtx();
    if (joinContext == null) {
      return true;
    }

    if (joinContext.getJoinPathColumnsOfTable(table) != null) {
      if (validColumns != null) {
        if (!validColumns.containsAll(joinContext.getJoinPathColumnsOfTable(table))) {
          return false;
        }
      }

      for (String joinPathColumn : joinContext.getJoinPathColumnsOfTable(table)) {
        joinPathColumn = joinPathColumn.toLowerCase();
        if (columnsOfTable.contains(joinPathColumn)) {
          return true;
        }
      }
    }
    return false;
  }
}
