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
import java.util.Collection;
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
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext.CandidateTable;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext.OptionalDimCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * This resolver prunes the candidate tables for following cases
 * 1. Queried columns are not part of candidate tables. Also Figures out if
 * queried column is not part of candidate table, but a denormalized field which
 * can reached through a reference
 * 2. Required join columns are not part of candidate tables
 * 3. Required source columns(join columns) for reaching a denormalized field, are
 * not part of candidate tables
 * 4. Required denormalized fields are not part of refered tables, there by all the
 * candidates which are using denormalized fields. 
 *
 */
public class CandidateTableResolver implements ContextRewriter {

  private static Log LOG = LogFactory.getLog(CandidateTableResolver.class.getName());
  // table to all its columns
  private static Map<AbstractCubeTable, Set<String>> cubeTabToCols =
      new HashMap<AbstractCubeTable, Set<String>>();
  // table to all its valid columns
  private static Map<AbstractCubeTable, List<String>> cubeTabToValidCols =
      new HashMap<AbstractCubeTable, List<String>>();
  private boolean qlEnabledMultiTableSelect;
  private boolean checkForQueriedColumns = true;

  public CandidateTableResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    qlEnabledMultiTableSelect = cubeql.getHiveConf().getBoolean(
        CubeQueryConfUtil.ENABLE_MULTI_TABLE_SELECT,
        CubeQueryConfUtil.DEFAULT_MULTI_TABLE_SELECT);
    if (checkForQueriedColumns) {
      populateCandidateTables(cubeql);
      resolveCandidateFactTables(cubeql);
      resolveCandidateDimTables(cubeql);
      checkForQueriedColumns = false;
    } else {
      // populate optional tables
      for (Dimension dim : cubeql.getOptionalDimensions()) {
        LOG.info("Populating optional dim:" + dim);
        populateDimTables(dim, cubeql, true);
      }
      checkForSourceReachabilityForDenormCandidates(cubeql);
      // check for joined columns and denorm columns on refered tables
      resolveCandidateFactTablesForJoins(cubeql);
      resolveCandidateDimTablesForJoinsAndDenorms(cubeql);
      checkForQueriedColumns = true;
    }
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
          CandidateFact cfact = new CandidateFact(fact, cubeql.getCube());
          cfact.enabledMultiTableSelect = qlEnabledMultiTableSelect;
          cubeql.getCandidateFactTables().add(cfact);
          cubeTabToCols.put(fact, fact.getAllFieldNames());
          cubeTabToValidCols.put(fact, fact.getValidColumns());
        }
        LOG.info("Populated candidate facts:" + cubeql.getCandidateFactTables());
      }

      if (cubeql.getDimensions().size() != 0) {
        for (Dimension dim : cubeql.getDimensions()) {
          populateDimTables(dim, cubeql, false);
        }
      }    
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  private void populateDimTables(Dimension dim, CubeQueryContext cubeql,
      boolean optional) throws SemanticException {
    try {
      Set<CandidateDim> candidates = new HashSet<CandidateDim>();
      cubeql.getCandidateDimTables().put(dim, candidates);
      List<CubeDimensionTable> dimtables = cubeql.getMetastoreClient().getAllDimensionTables(dim);
      if (dimtables.isEmpty()) {
        if (!optional) {
          throw new SemanticException(ErrorMsg.NO_CANDIDATE_DIM_AVAILABLE,
              dim.getName(), "Dimension tables do not exist");
        } else {
          LOG.info("Not considering optional dimension " + dim + " as," +
              " No dimension tables exist");
          removeOptionalDim(cubeql, dim);
        }
      }
      for (CubeDimensionTable dimtable : dimtables) {
        CandidateDim cdim = new CandidateDim(dimtable, dim);
        candidates.add(cdim);
        cubeTabToCols.put(dimtable, dimtable.getAllFieldNames());
      }
      LOG.info("Populated candidate dims:" + cubeql.getCandidateDimTables().get(dim) + " for " + dim);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  private void removeOptionalDim(CubeQueryContext cubeql, Dimension dim) {
    OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().remove(dim);
    // remove all the depending candiate table as well
    for (CandidateTable candidate : optdim.requiredForCandidates) {
      if (candidate instanceof CandidateFact) {
        LOG.info("Not considering fact:" + candidate + " as refered table does not have any valid dimtables");
        cubeql.getCandidateFactTables().remove(candidate);
        cubeql.addFactPruningMsgs(((CandidateFact)candidate).fact,
            new CandidateTablePruneCause(candidate.getName(),
                CubeTableCause.INVALID_DENORM_TABLE));
      } else {
        LOG.info("Not considering dimtable:" + candidate + " as refered table does not have any valid dimtables");
        cubeql.getCandidateDimTables().get(((CandidateDim)candidate).dimtable.getDimName()).remove(candidate);
        cubeql.addDimPruningMsgs((Dimension)candidate.getBaseTable(),
            (CubeDimensionTable)candidate.getTable(), new CandidateTablePruneCause(
                candidate.getName(), CubeTableCause.INVALID_DENORM_TABLE));
      }
    }
  }

  private void resolveCandidateFactTables(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getCube() != null) {
      String str = cubeql.getHiveConf().get(CubeQueryConfUtil.getValidFactTablesKey(
          cubeql.getCube().getName()));
      List<String> validFactTables = StringUtils.isBlank(str) ? null :
        Arrays.asList(StringUtils.split(str.toLowerCase(), ","));
      Set<String> cubeColsQueried = cubeql.getColumnsQueried(cubeql.getCube().getName());

      // Remove fact tables based on columns in the query
      for (Iterator<CandidateFact> i = cubeql.getCandidateFactTables().iterator();
          i.hasNext();) {
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
                // check if it available as reference, if not remove the candidate
                if (!cubeql.getDenormCtx().addRefUsage(cfact, col, cubeql.getCube().getName())) {
                  LOG.info("Not considering fact table:" + fact +
                      " as column " + col + " is not valid");
                  cubeql.addFactPruningMsgs(fact, new CandidateTablePruneCause(
                      fact.getName(), CubeTableCause.COLUMN_NOT_VALID));
                  i.remove();
                  break;
                }
              }
            } else if(!factCols.contains(col.toLowerCase())) {
              // check if it available as reference, if not remove the candidate
              if (!cubeql.getDenormCtx().addRefUsage(cfact, col, cubeql.getCube().getName())) {
                LOG.info("Not considering fact table:" + fact +
                    " as column " + col + " is not available");
                cubeql.addFactPruningMsgs(fact, new CandidateTablePruneCause(
                    fact.getName(), CubeTableCause.COLUMN_NOT_FOUND));
                i.remove();
                break;
              }
            }
          }
        }
      }
      if (cubeql.getCandidateFactTables().size() == 0) {
        throw new SemanticException(ErrorMsg.NO_FACT_HAS_COLUMN,
            cubeColsQueried.toString());
      }
    }
  }

  private void resolveCandidateDimTablesForJoinsAndDenorms(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getAutoJoinCtx() == null) {
      return;
    }
    Set<Dimension> allDims = new HashSet<Dimension>(cubeql.getDimensions());
    allDims.addAll(cubeql.getOptionalDimensions());
    for (Dimension dim : allDims) {
      if (cubeql.getCandidateDimTables().get(dim) != null && !cubeql.getCandidateDimTables().get(dim).isEmpty()) {
        for (Iterator<CandidateDim> i = cubeql.getCandidateDimTables().get(dim).iterator();
            i.hasNext();) {
          CandidateDim cdim = i.next();
          CubeDimensionTable dimtable = cdim.dimtable;
          // go over the join columns accessed in the query and find out which tables
          // can participate in join
          // for each join path check for columns involved in path
          for (Map.Entry<Dimension, Map<AbstractCubeTable, List<String>>> joincolumnsEntry : 
            cubeql.getAutoJoinCtx().getAlljoinPathColumns().entrySet()) {
            Dimension reachableDim = joincolumnsEntry.getKey();
            OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(reachableDim);
            Collection<String> colSet = joincolumnsEntry.getValue().get((AbstractCubeTable) dim);

            if (!checkForColumnExists(cdim, colSet)) {
              if (optdim == null ||
                  optdim.isRequiredInJoinChain || 
                  (optdim != null && optdim.requiredForCandidates.contains(cdim))) {
                i.remove();
                LOG.info("Not considering dimtable:" + dimtable +" as its columns are" +
                    " not part of any join paths. Join columns:" + colSet);
                cubeql.addDimPruningMsgs(dim, dimtable, new CandidateTablePruneCause(
                    dimtable.getName(), CubeTableCause.NO_COLUMN_PART_OF_A_JOIN_PATH));
                break;
              }
            }
          }

          // go over the referenced columns accessed in the query and find out which tables
          // can participate
          if (!checkForColumnExists(cdim, cubeql.getOptionalDimensionMap().get(dim).colQueried)) {
            i.remove();
            LOG.info("Not considering optional dimtable:" + dimtable +" as its denorm fields do not exist." +
                " Denorm fields:" + cubeql.getOptionalDimensionMap().get(dim).colQueried);
            cubeql.addDimPruningMsgs(dim, dimtable, new CandidateTablePruneCause(
                dimtable.getName(), CubeTableCause.NO_COLUMN_PART_OF_A_JOIN_PATH));
          }
        }

        if (cubeql.getCandidateDimTables().get(dim).size() == 0) {
          OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(dim);
          if ((cubeql.getDimensions() != null && cubeql.getDimensions().contains(dim))
              || (optdim != null && optdim.isRequiredInJoinChain)) {
            throw new SemanticException(ErrorMsg.NO_DIM_HAS_COLUMN,
                dim.getName(),
                cubeql.getAutoJoinCtx().getJoinPathColumnsOfTable(dim).toString());
          } else {
            // remove it from optional tables
            LOG.info("Not considering optional dimension " + dim + " as," +
                " No dimension table has the queried columns:" + 
                optdim.colQueried +
                " Clearing the required for candidates:" + optdim.requiredForCandidates);
            removeOptionalDim(cubeql, dim);
          }
        }
      }
    }
  }

  private void resolveCandidateFactTablesForJoins(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getAutoJoinCtx() == null) {
      return;
    }
    if (cubeql.getCube() != null && !cubeql.getCandidateFactTables().isEmpty()) {
      for (Iterator<CandidateFact> i = cubeql.getCandidateFactTables().iterator();
          i.hasNext();) {
        CandidateFact cfact = i.next();
        CubeFactTable fact = cfact.fact;

        // for each join path check for columns involved in path
        for (Map.Entry<Dimension, Map<AbstractCubeTable, List<String>>> joincolumnsEntry : 
          cubeql.getAutoJoinCtx().getAlljoinPathColumns().entrySet()) {
          Dimension reachableDim = joincolumnsEntry.getKey();
          OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(reachableDim);
          Collection<String> colSet = joincolumnsEntry.getValue().get((AbstractCubeTable) cubeql.getCube());

          if (!checkForColumnExists(cfact, colSet)) {
            if (optdim == null ||
                optdim.isRequiredInJoinChain || 
                (optdim != null && optdim.requiredForCandidates.contains(cfact))) {
              i.remove();
              LOG.info("Not considering fact table:" + fact + " as it does not have columns" +
                  " in any of the join paths. Join columns:" + colSet);
              cubeql.addFactPruningMsgs(fact,
                  new CandidateTablePruneCause(fact.getName(), CubeTableCause.NO_COLUMN_PART_OF_A_JOIN_PATH));
              break;
            }
          }
        }
      }
      if (cubeql.getCandidateFactTables().size() == 0) {
        throw new SemanticException(ErrorMsg.NO_FACT_HAS_COLUMN,
            cubeql.getAutoJoinCtx().getJoinPathColumnsOfTable(
                (AbstractCubeTable) cubeql.getCube()).toString());
      }
    }
  }

  private void checkForSourceReachabilityForDenormCandidates(CubeQueryContext cubeql) {
    if (cubeql.getOptionalDimensionMap().isEmpty()) {
      return;
    }
    if (cubeql.getAutoJoinCtx() == null) {
      Set<Dimension> optionaldims = new HashSet<Dimension>(cubeql.getOptionalDimensions());
      for (Dimension dim : optionaldims) {
       LOG.info("Not considering optional dimension " + dim + " as," +
          " automatic join resolver is disbled ");
       removeOptionalDim(cubeql, dim);
      }
      return;
    }
    // check for source columns for denorm columns
    for (Map.Entry<Dimension,OptionalDimCtx> optdimEntry : cubeql.getOptionalDimensionMap().entrySet()) {
      Dimension dim = optdimEntry.getKey();
      OptionalDimCtx optdim = optdimEntry.getValue();
      for (CandidateTable candidate : optdim.requiredForCandidates) {
        List<String> colSet = cubeql.getAutoJoinCtx().getJoinPathColumnsOfTable(dim).get(candidate.getBaseTable()); 
        if (!checkForColumnExists(candidate, colSet)) {
          if (candidate instanceof CandidateFact) {
            LOG.info("Not considering fact:" + candidate + " as columns do not exist. Required cols:" + colSet);
            cubeql.getCandidateFactTables().remove(candidate);
            cubeql.addFactPruningMsgs(((CandidateFact)candidate).fact,
                new CandidateTablePruneCause(candidate.getName(),
                    CubeTableCause.COLUMN_NOT_FOUND));
          } else {
            LOG.info("Not considering dimtable:" + candidate + " as columns do not exist. Required cols:" + colSet);
            cubeql.getCandidateDimTables().get(((CandidateDim)candidate).dimtable.getDimName()).remove(candidate);
            cubeql.addDimPruningMsgs((Dimension)candidate.getBaseTable(),
                (CubeDimensionTable)candidate.getTable(), new CandidateTablePruneCause(
                    candidate.getName(), CubeTableCause.COLUMN_NOT_FOUND));
          }
        }
      }
    }
  }

  private void resolveCandidateDimTables(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getDimensions().size() != 0) {
      for (Dimension dim : cubeql.getDimensions()) {
        // go over the columns accessed in the query and find out which tables
        // can answer the query
        for (Iterator<CandidateDim> i = cubeql.getCandidateDimTables().get(dim).iterator();
            i.hasNext();) {
          CandidateDim cdim = i.next();
          CubeDimensionTable dimtable = cdim.dimtable;

          Set<String> dimCols = cubeTabToCols.get(dimtable);

          if (cubeql.getColumnsQueried(dim.getName()) != null) {
            for (String col : cubeql.getColumnsQueried(dim.getName())) {
              if (!dimCols.contains(col.toLowerCase())) {
                // check if it available as reference, if not remove the candidate
                if (cubeql.getDenormCtx().addRefUsage(cdim, col, dim.getName())) {
                  LOG.info("Not considering dimtable:" + dimtable +
                      " as column " + col + " is not available");
                  cubeql.addDimPruningMsgs(dim, dimtable, new CandidateTablePruneCause(
                      dimtable.getName(), CubeTableCause.COLUMN_NOT_FOUND));
                  i.remove();
                  break;
                }
              }
            }
          }
        }

        if (cubeql.getCandidateDimTables().get(dim).size() == 0) {
          throw new SemanticException(ErrorMsg.NO_DIM_HAS_COLUMN,
              dim.getName(), cubeql.getColumnsQueried(dim.getName()).toString());
        }
      }
    }
  }

  public static boolean checkForColumnExists(CandidateTable table,
      Collection<String> colSet) {
    if (colSet == null || colSet.isEmpty()) {
      return true;
    }
    Collection<String> tblCols = getAllColumns(table.getTable());
    LOG.warn("columns for table:" +table + tblCols);
    for (String column : colSet) {
      if (tblCols.contains(column)) {
        return true;
      }
    }
    return false;
  }

  public static Collection<String> getAllColumns(AbstractCubeTable table) {
    if (cubeTabToValidCols.get(table) != null) {
      return cubeTabToValidCols.get(table);
    } else {
      return cubeTabToCols.get(table);
    }
  }
}
