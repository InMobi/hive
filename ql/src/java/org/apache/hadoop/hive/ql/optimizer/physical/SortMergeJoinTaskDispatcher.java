/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.optimizer.MapJoinProcessor;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverCommonJoin;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverCommonJoin.ConditionalResolverCommonJoinCtx;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;

/**
 * Iterator over each task. If the task has a smb join, convert the task to a conditional task.
 * The conditional task will first try all mapjoin possibilities, and go the the smb join if the
 * mapjoin fails. The smb join will be a backup task for all the mapjoin tasks.
 */
public class SortMergeJoinTaskDispatcher extends AbstractJoinTaskDispatcher implements Dispatcher {

  public SortMergeJoinTaskDispatcher(PhysicalContext context) {
    super(context);
  }

  // Convert the work in the SMB plan to a regular join
  // Note that the operator tree is not fixed, only the path/alias mappings in the
  // plan are fixed. The operator tree will still contain the SMBJoinOperator
  private void genSMBJoinWork(MapWork currWork, SMBMapJoinOperator smbJoinOp) {
    // Remove the paths which are not part of aliasToPartitionInfo
    Map<String, PartitionDesc> aliasToPartitionInfo = currWork.getAliasToPartnInfo();
    List<String> removePaths = new ArrayList<String>();

    for (Map.Entry<String, ArrayList<String>> entry : currWork.getPathToAliases().entrySet()) {
      boolean keepPath = false;
      for (String alias : entry.getValue()) {
        if (aliasToPartitionInfo.containsKey(alias)) {
          keepPath = true;
          break;
        }
      }

      // Remove if the path is not present
      if (!keepPath) {
        removePaths.add(entry.getKey());
      }
    }

    List<String> removeAliases = new ArrayList<String>();
    for (String removePath : removePaths) {
      removeAliases.addAll(currWork.getPathToAliases().get(removePath));
      currWork.getPathToAliases().remove(removePath);
      currWork.getPathToPartitionInfo().remove(removePath);
    }

    for (String alias : removeAliases) {
      currWork.getAliasToPartnInfo().remove(alias);
      currWork.getAliasToWork().remove(alias);
    }

    // Get the MapredLocalWork
    MapredLocalWork localWork = smbJoinOp.getConf().getLocalWork();

    for (Map.Entry<String, Operator<? extends OperatorDesc>> entry : localWork.getAliasToWork()
        .entrySet()) {
      String alias = entry.getKey();
      Operator<? extends OperatorDesc> op = entry.getValue();
      FetchWork fetchWork = localWork.getAliasToFetchWork().get(alias);

      // Add the entry in mapredwork
      currWork.getAliasToWork().put(alias, op);

      PartitionDesc partitionInfo = currWork.getAliasToPartnInfo().get(alias);
      if (fetchWork.getTblDir() != null) {
        currWork.mergeAliasedInput(alias, fetchWork.getTblDir(), partitionInfo);
      } else {
        for (String pathDir : fetchWork.getPartDir()) {
          currWork.mergeAliasedInput(alias, pathDir, partitionInfo);
        }
      }
    }

    // Remove the dummy store operator from the tree
    for (Operator<? extends OperatorDesc> parentOp : smbJoinOp.getParentOperators()) {
      if (parentOp instanceof DummyStoreOperator) {
        Operator<? extends OperatorDesc> grandParentOp = parentOp.getParentOperators().get(0);
        smbJoinOp.replaceParent(parentOp, grandParentOp);
        grandParentOp.setChildOperators(parentOp.getChildOperators());
        parentOp.setParentOperators(null);
        parentOp.setParentOperators(null);
      }
    }
  }

  /*
   * Convert the work containing to sort-merge join into a work, as if it had a regular join.
   * Note that the operator tree is not changed - is still contains the SMB join, but the
   * plan is changed (aliasToWork etc.) to contain all the paths as if it was a regular join.
   */
  private MapredWork convertSMBWorkToJoinWork(MapredWork currWork, SMBMapJoinOperator oldSMBJoinOp)
      throws SemanticException {
    try {
      // deep copy a new mapred work
      MapredWork currJoinWork = Utilities.clonePlan(currWork);
      SMBMapJoinOperator newSMBJoinOp = getSMBMapJoinOp(currJoinWork);

      // Add the row resolver for the new operator
      Map<Operator<? extends OperatorDesc>, OpParseContext> opParseContextMap =
          physicalContext.getParseContext().getOpParseCtx();
      opParseContextMap.put(newSMBJoinOp, opParseContextMap.get(oldSMBJoinOp));
      // change the newly created map-red plan as if it was a join operator
      genSMBJoinWork(currJoinWork.getMapWork(), newSMBJoinOp);
      return currJoinWork;
    } catch (Exception e) {
      e.printStackTrace();
      throw new SemanticException("Generate Map Join Task Error: " + e.getMessage());
    }
  }

  // create map join task and set big table as bigTablePosition
  private ObjectPair<MapRedTask, String> convertSMBTaskToMapJoinTask(MapredWork origWork,
      int bigTablePosition,
      SMBMapJoinOperator smbJoinOp,
      QBJoinTree joinTree)
      throws UnsupportedEncodingException, SemanticException {
    // deep copy a new mapred work
    MapredWork newWork = Utilities.clonePlan(origWork);
    // create a mapred task for this work
    MapRedTask newTask = (MapRedTask) TaskFactory.get(newWork, physicalContext
        .getParseContext().getConf());
    // generate the map join operator; already checked the map join
    MapJoinOperator newMapJoinOp =
        getMapJoinOperator(newTask, newWork, smbJoinOp, joinTree, bigTablePosition);

    // The reducer needs to be restored - Consider a query like:
    // select count(*) FROM bucket_big a JOIN bucket_small b ON a.key = b.key;
    // The reducer contains a groupby, which needs to be restored.
    ReduceWork rWork = newWork.getReduceWork();

    // create the local work for this plan
    String bigTableAlias =
        MapJoinProcessor.genLocalWorkForMapJoin(newWork, newMapJoinOp, bigTablePosition);

    // restore the reducer
    newWork.setReduceWork(rWork);
    return new ObjectPair<MapRedTask, String>(newTask, bigTableAlias);
  }

  private boolean isEligibleForOptimization(SMBMapJoinOperator originalSMBJoinOp) {
    if (originalSMBJoinOp == null) {
      return false;
    }

    // Only create a map-join if the user explicitly gave a join (without a mapjoin hint)
    if (!originalSMBJoinOp.isConvertedAutomaticallySMBJoin()) {
      return false;
    }

    Operator<? extends OperatorDesc> currOp = originalSMBJoinOp;
    while (true) {
      if (currOp.getChildOperators() == null) {
        if (currOp instanceof FileSinkOperator) {
          FileSinkOperator fsOp = (FileSinkOperator)currOp;
          // The query has enforced that a sort-merge join should be performed.
          // For more details, look at 'removedReduceSinkBucketSort' in FileSinkDesc.java
          return !fsOp.getConf().isRemovedReduceSinkBucketSort();
        }

        // If it contains a reducer, the optimization is always on.
        // Since there exists a reducer, the sorting/bucketing properties due to the
        // sort-merge join operator are lost anyway. So, the plan cannot be wrong by
        // changing the sort-merge join to a map-join
        if (currOp instanceof ReduceSinkOperator) {
          return true;
        }
        return false;
      }

      if (currOp.getChildOperators().size() > 1) {
        return true;
      }

      currOp = currOp.getChildOperators().get(0);
    }
  }

  @Override
  public Task<? extends Serializable> processCurrentTask(MapRedTask currTask,
      ConditionalTask conditionalTask, Context context)
      throws SemanticException {

    // whether it contains a sort merge join operator
    MapredWork currWork = currTask.getWork();
    SMBMapJoinOperator originalSMBJoinOp = getSMBMapJoinOp(currWork);
    if (!isEligibleForOptimization(originalSMBJoinOp)) {
      return null;
    }

    currTask.setTaskTag(Task.CONVERTED_SORTMERGEJOIN);

    // get parseCtx for this Join Operator
    ParseContext parseCtx = physicalContext.getParseContext();
    QBJoinTree joinTree = parseCtx.getSmbMapJoinContext().get(originalSMBJoinOp);

    // Convert the work containing to sort-merge join into a work, as if it had a regular join.
    // Note that the operator tree is not changed - is still contains the SMB join, but the
    // plan is changed (aliasToWork etc.) to contain all the paths as if it was a regular join.
    // This is used to convert the plan to a map-join, and then the original SMB join plan is used
    // as a backup task.
    MapredWork currJoinWork = convertSMBWorkToJoinWork(currWork, originalSMBJoinOp);
    SMBMapJoinOperator newSMBJoinOp = getSMBMapJoinOp(currJoinWork);

    currWork.getMapWork().setOpParseCtxMap(parseCtx.getOpParseCtx());
    currWork.getMapWork().setJoinTree(joinTree);
    currJoinWork.getMapWork().setOpParseCtxMap(parseCtx.getOpParseCtx());
    currJoinWork.getMapWork().setJoinTree(joinTree);

    // create conditional work list and task list
    List<Serializable> listWorks = new ArrayList<Serializable>();
    List<Task<? extends Serializable>> listTasks = new ArrayList<Task<? extends Serializable>>();

    // create alias to task mapping and alias to input file mapping for resolver
    HashMap<String, Task<? extends Serializable>> aliasToTask =
        new HashMap<String, Task<? extends Serializable>>();
    // Note that pathToAlias will behave as if the original plan was a join plan
    HashMap<String, ArrayList<String>> pathToAliases = currJoinWork.getMapWork().getPathToAliases();

    // generate a map join task for the big table
    SMBJoinDesc originalSMBJoinDesc = originalSMBJoinOp.getConf();
    Byte[] order = originalSMBJoinDesc.getTagOrder();
    int numAliases = order.length;
    Set<Integer> bigTableCandidates =
        MapJoinProcessor.getBigTableCandidates(originalSMBJoinDesc.getConds());

    HashMap<String, Long> aliasToSize = new HashMap<String, Long>();
    Configuration conf = context.getConf();
    try {
      long aliasTotalKnownInputSize = getTotalKnownInputSize(context, currJoinWork.getMapWork(),
          pathToAliases, aliasToSize);

      long ThresholdOfSmallTblSizeSum = HiveConf.getLongVar(conf,
          HiveConf.ConfVars.HIVESMALLTABLESFILESIZE);

      for (int bigTablePosition = 0; bigTablePosition < numAliases; bigTablePosition++) {
        // this table cannot be big table
        if (!bigTableCandidates.contains(bigTablePosition)) {
          continue;
        }

        // create map join task for the given big table position
        ObjectPair<MapRedTask, String> newTaskAlias = convertSMBTaskToMapJoinTask(
            currJoinWork, bigTablePosition, newSMBJoinOp, joinTree);
        MapRedTask newTask = newTaskAlias.getFirst();
        String bigTableAlias = newTaskAlias.getSecond();

        Long aliasKnownSize = aliasToSize.get(bigTableAlias);
        if (aliasKnownSize != null && aliasKnownSize.longValue() > 0) {
          long smallTblTotalKnownSize = aliasTotalKnownInputSize
              - aliasKnownSize.longValue();
          if (smallTblTotalKnownSize > ThresholdOfSmallTblSizeSum) {
            // this table is not good to be a big table.
            continue;
          }
        }

        // add into conditional task
        listWorks.add(newTask.getWork());
        listTasks.add(newTask);
        newTask.setTaskTag(Task.CONVERTED_MAPJOIN);

        // set up backup task
        newTask.setBackupTask(currTask);
        newTask.setBackupChildrenTasks(currTask.getChildTasks());

        // put the mapping alias to task
        aliasToTask.put(bigTableAlias, newTask);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new SemanticException("Generate Map Join Task Error: ", e);
    }

    // insert current common join task to conditional task
    listWorks.add(currTask.getWork());
    listTasks.add(currTask);
    // clear JoinTree and OP Parse Context
    currWork.getMapWork().setOpParseCtxMap(null);
    currWork.getMapWork().setJoinTree(null);

    // create conditional task and insert conditional task into task tree
    ConditionalWork cndWork = new ConditionalWork(listWorks);
    ConditionalTask cndTsk = (ConditionalTask) TaskFactory.get(cndWork, parseCtx.getConf());
    cndTsk.setListTasks(listTasks);

    // set resolver and resolver context
    cndTsk.setResolver(new ConditionalResolverCommonJoin());
    ConditionalResolverCommonJoinCtx resolverCtx = new ConditionalResolverCommonJoinCtx();
    resolverCtx.setPathToAliases(pathToAliases);
    resolverCtx.setAliasToKnownSize(aliasToSize);
    resolverCtx.setAliasToTask(aliasToTask);
    resolverCtx.setCommonJoinTask(currTask);
    resolverCtx.setLocalTmpDir(context.getLocalScratchDir(false));
    resolverCtx.setHdfsTmpDir(context.getMRScratchDir());
    cndTsk.setResolverCtx(resolverCtx);

    // replace the current task with the new generated conditional task
    replaceTaskWithConditionalTask(currTask, cndTsk, physicalContext);
    return cndTsk;
  }

  /**
   * If a join/union is followed by a SMB join, this cannot be converted to a conditional task.
   */
  private boolean reducerAllowedSMBJoinOp(Operator<? extends OperatorDesc> reducer) {
    while (reducer != null) {
      if (!reducer.opAllowedBeforeSortMergeJoin()) {
        return false;
      }

      List<Operator<? extends OperatorDesc>> childOps = reducer.getChildOperators();
      if ((childOps == null) || (childOps.isEmpty())) {
        return true;
      }

      // multi-table inserts not supported
      if (childOps.size() > 1) {
        return false;
      }
      reducer = childOps.get(0);
    }

    return true;
  }

  private SMBMapJoinOperator getSMBMapJoinOp(Operator<? extends OperatorDesc> currOp,
      Operator<? extends OperatorDesc> reducer) {
    SMBMapJoinOperator ret = null;
    while (true) {
      if (currOp instanceof SMBMapJoinOperator) {
        if (ret != null) {
          return null;
        }
        ret = (SMBMapJoinOperator) currOp;
      }

      // Does any operator in the tree stop the task from being converted to a conditional task
      if (!currOp.opAllowedBeforeSortMergeJoin()) {
        return null;
      }

      List<Operator<? extends OperatorDesc>> childOps = currOp.getChildOperators();
      if ((childOps == null) || (childOps.isEmpty())) {
        return reducerAllowedSMBJoinOp(reducer) ? ret : null;
      }

      // multi-table inserts not supported
      if (childOps.size() > 1) {
        return null;
      }
      currOp = childOps.get(0);
    }
  }

  private SMBMapJoinOperator getSMBMapJoinOp(MapredWork work) throws SemanticException {
    if (work != null && work.getReduceWork() != null) {
      Operator<? extends OperatorDesc> reducer = work.getReduceWork().getReducer();
      for (Operator<? extends OperatorDesc> op : work.getMapWork().getAliasToWork().values()) {
        SMBMapJoinOperator smbMapJoinOp = getSMBMapJoinOp(op, reducer);
        if (smbMapJoinOp != null) {
          return smbMapJoinOp;
        }
      }
    }
    return null;
  }

  private MapJoinOperator getMapJoinOperator(MapRedTask task,
      MapredWork work,
      SMBMapJoinOperator oldSMBJoinOp,
      QBJoinTree joinTree,
      int mapJoinPos) throws SemanticException {
    SMBMapJoinOperator newSMBJoinOp = getSMBMapJoinOp(task.getWork());

    // Add the row resolver for the new operator
    Map<Operator<? extends OperatorDesc>, OpParseContext> opParseContextMap =
        physicalContext.getParseContext().getOpParseCtx();
    opParseContextMap.put(newSMBJoinOp, opParseContextMap.get(oldSMBJoinOp));

    // generate the map join operator
    return MapJoinProcessor.convertSMBJoinToMapJoin(opParseContextMap, newSMBJoinOp,
        joinTree, mapJoinPos, true);
  }
}
