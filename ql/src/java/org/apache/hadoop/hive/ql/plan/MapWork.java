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

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.optimizer.physical.BucketingSortingCtx.BucketCol;
import org.apache.hadoop.hive.ql.optimizer.physical.BucketingSortingCtx.SortCol;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.mapred.JobConf;

/**
 * MapWork represents all the information used to run a map task on the cluster.
 * It is first used when the query planner breaks the logical plan into tasks and
 * used throughout physical optimization to track map-side operator plans, input
 * paths, aliases, etc.
 *
 * ExecDriver will serialize the contents of this class and make sure it is
 * distributed on the cluster. The ExecMapper will ultimately deserialize this
 * class on the data nodes and setup it's operator pipeline accordingly.
 *
 * This class is also used in the explain command any property with the 
 * appropriate annotation will be displayed in the explain output.
 */
@SuppressWarnings({"serial", "deprecation"})
public class MapWork extends BaseWork {

  private static transient final Log LOG = LogFactory.getLog(MapWork.class);

  private boolean hadoopSupportsSplittable;

  // use LinkedHashMap to make sure the iteration order is
  // deterministic, to ease testing
  private LinkedHashMap<String, ArrayList<String>> pathToAliases = new LinkedHashMap<String, ArrayList<String>>();

  private LinkedHashMap<String, PartitionDesc> pathToPartitionInfo = new LinkedHashMap<String, PartitionDesc>();

  private LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork = new LinkedHashMap<String, Operator<? extends OperatorDesc>>();

  private LinkedHashMap<String, PartitionDesc> aliasToPartnInfo = new LinkedHashMap<String, PartitionDesc>();

  private HashMap<String, SplitSample> nameToSplitSample = new LinkedHashMap<String, SplitSample>();

  // If this map task has a FileSinkOperator, and bucketing/sorting metadata can be
  // inferred about the data being written by that operator, these are mappings from the directory
  // that operator writes into to the bucket/sort columns for that data.
  private final Map<String, List<BucketCol>> bucketedColsByDirectory =
      new HashMap<String, List<BucketCol>>();
  private final Map<String, List<SortCol>> sortedColsByDirectory =
      new HashMap<String, List<SortCol>>();

  private MapredLocalWork mapLocalWork;
  private String tmpHDFSFileURI;

  private String inputformat;

  private String indexIntermediateFile;

  private Integer numMapTasks;
  private Long maxSplitSize;
  private Long minSplitSize;
  private Long minSplitSizePerNode;
  private Long minSplitSizePerRack;

  //use sampled partitioning
  private int samplingType;

  public static final int SAMPLING_ON_PREV_MR = 1;  // todo HIVE-3841
  public static final int SAMPLING_ON_START = 2;    // sampling on task running

  // the following two are used for join processing
  private QBJoinTree joinTree;
  private LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtxMap;

  private boolean mapperCannotSpanPartns;

  // used to indicate the input is sorted, and so a BinarySearchRecordReader shoudl be used
  private boolean inputFormatSorted = false;

  private transient boolean useBucketizedHiveInputFormat;

  public MapWork() {
  }

  @Explain(displayName = "Path -> Alias", normalExplain = false)
  public LinkedHashMap<String, ArrayList<String>> getPathToAliases() {
    return pathToAliases;
  }

  public void setPathToAliases(
      final LinkedHashMap<String, ArrayList<String>> pathToAliases) {
    this.pathToAliases = pathToAliases;
  }

  /**
   * This is used to display and verify output of "Path -> Alias" in test framework.
   *
   * QTestUtil masks "Path -> Alias" and makes verification impossible.
   * By keeping "Path -> Alias" intact and adding a new display name which is not
   * masked by QTestUtil by removing prefix.
   *
   * Notes: we would still be masking for intermediate directories.
   *
   * @return
   */
  @Explain(displayName = "Truncated Path -> Alias", normalExplain = false)
  public Map<String, ArrayList<String>> getTruncatedPathToAliases() {
    Map<String, ArrayList<String>> trunPathToAliases = new LinkedHashMap<String,
        ArrayList<String>>();
    Iterator<Entry<String, ArrayList<String>>> itr = this.pathToAliases.entrySet().iterator();
    while (itr.hasNext()) {
      final Entry<String, ArrayList<String>> entry = itr.next();
      String origiKey = entry.getKey();
      String newKey = PlanUtils.removePrefixFromWarehouseConfig(origiKey);
      ArrayList<String> value = entry.getValue();
      trunPathToAliases.put(newKey, value);
    }
    return trunPathToAliases;
  }

  @Explain(displayName = "Path -> Partition", normalExplain = false)
  public LinkedHashMap<String, PartitionDesc> getPathToPartitionInfo() {
    return pathToPartitionInfo;
  }

  public void setPathToPartitionInfo(
      final LinkedHashMap<String, PartitionDesc> pathToPartitionInfo) {
    this.pathToPartitionInfo = pathToPartitionInfo;
  }

  /**
   * Derive additional attributes to be rendered by EXPLAIN.
   */
  public void deriveExplainAttributes() {
    if (pathToPartitionInfo != null) {
      for (Map.Entry<String, PartitionDesc> entry : pathToPartitionInfo
          .entrySet()) {
        entry.getValue().deriveBaseFileName(entry.getKey());
      }
    }
    if (mapLocalWork != null) {
      mapLocalWork.deriveExplainAttributes();
    }
  }

  /**
   * @return the aliasToPartnInfo
   */
  public LinkedHashMap<String, PartitionDesc> getAliasToPartnInfo() {
    return aliasToPartnInfo;
  }

  /**
   * @param aliasToPartnInfo
   *          the aliasToPartnInfo to set
   */
  public void setAliasToPartnInfo(
      LinkedHashMap<String, PartitionDesc> aliasToPartnInfo) {
    this.aliasToPartnInfo = aliasToPartnInfo;
  }

  @Explain(displayName = "Alias -> Map Operator Tree")
  public LinkedHashMap<String, Operator<? extends OperatorDesc>> getAliasToWork() {
    return aliasToWork;
  }

  public void setAliasToWork(
      final LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork) {
    this.aliasToWork = aliasToWork;
  }

  /**
   * @return the mapredLocalWork
   */
  @Explain(displayName = "Local Work")
  public MapredLocalWork getMapLocalWork() {
    return mapLocalWork;
  }

  /**
   * @param mapLocalWork
   *          the mapredLocalWork to set
   */
  public void setMapLocalWork(final MapredLocalWork mapLocalWork) {
    this.mapLocalWork = mapLocalWork;
  }


  @Explain(displayName = "Split Sample")
  public HashMap<String, SplitSample> getNameToSplitSample() {
    return nameToSplitSample;
  }

  public void setNameToSplitSample(HashMap<String, SplitSample> nameToSplitSample) {
    this.nameToSplitSample = nameToSplitSample;
  }

  public Integer getNumMapTasks() {
    return numMapTasks;
  }

  public void setNumMapTasks(Integer numMapTasks) {
    this.numMapTasks = numMapTasks;
  }

  @SuppressWarnings("nls")
  public void addMapWork(String path, String alias, Operator<?> work,
      PartitionDesc pd) {
    ArrayList<String> curAliases = pathToAliases.get(path);
    if (curAliases == null) {
      assert (pathToPartitionInfo.get(path) == null);
      curAliases = new ArrayList<String>();
      pathToAliases.put(path, curAliases);
      pathToPartitionInfo.put(path, pd);
    } else {
      assert (pathToPartitionInfo.get(path) != null);
    }

    for (String oneAlias : curAliases) {
      if (oneAlias.equals(alias)) {
        throw new RuntimeException("Multiple aliases named: " + alias
            + " for path: " + path);
      }
    }
    curAliases.add(alias);

    if (aliasToWork.get(alias) != null) {
      throw new RuntimeException("Existing work for alias: " + alias);
    }
    aliasToWork.put(alias, work);
  }

  public boolean isInputFormatSorted() {
    return inputFormatSorted;
  }

  public void setInputFormatSorted(boolean inputFormatSorted) {
    this.inputFormatSorted = inputFormatSorted;
  }

  public void resolveDynamicPartitionStoredAsSubDirsMerge(HiveConf conf, Path path,
      TableDesc tblDesc, ArrayList<String> aliases, PartitionDesc partDesc) {
    pathToAliases.put(path.toString(), aliases);
    pathToPartitionInfo.put(path.toString(), partDesc);
  }

  /**
   * For each map side operator - stores the alias the operator is working on
   * behalf of in the operator runtime state. This is used by reduce sink
   * operator - but could be useful for debugging as well.
   */
  private void setAliases() {
    if(aliasToWork == null) {
      return;
    }
    for (String oneAlias : aliasToWork.keySet()) {
      aliasToWork.get(oneAlias).setAlias(oneAlias);
    }
  }

  @Override
  protected List<Operator<?>> getAllRootOperators() {
    ArrayList<Operator<?>> opList = new ArrayList<Operator<?>>();

    Map<String, ArrayList<String>> pa = getPathToAliases();
    if (pa != null) {
      for (List<String> ls : pa.values()) {
        for (String a : ls) {
          Operator<?> op = getAliasToWork().get(a);
          if (op != null ) {
            opList.add(op);
          }
        }
      }
    }
    return opList;
  }

  public void mergeAliasedInput(String alias, String pathDir, PartitionDesc partitionInfo) {
    ArrayList<String> aliases = pathToAliases.get(pathDir);
    if (aliases == null) {
      aliases = new ArrayList<String>(Arrays.asList(alias));
      pathToAliases.put(pathDir, aliases);
      pathToPartitionInfo.put(pathDir, partitionInfo);
    } else {
      aliases.add(alias);
    }
  }

  public void initialize() {
    setAliases();
  }

  public Long getMaxSplitSize() {
    return maxSplitSize;
  }

  public void setMaxSplitSize(Long maxSplitSize) {
    this.maxSplitSize = maxSplitSize;
  }

  public Long getMinSplitSize() {
    return minSplitSize;
  }

  public void setMinSplitSize(Long minSplitSize) {
    this.minSplitSize = minSplitSize;
  }

  public Long getMinSplitSizePerNode() {
    return minSplitSizePerNode;
  }

  public void setMinSplitSizePerNode(Long minSplitSizePerNode) {
    this.minSplitSizePerNode = minSplitSizePerNode;
  }

  public Long getMinSplitSizePerRack() {
    return minSplitSizePerRack;
  }

  public void setMinSplitSizePerRack(Long minSplitSizePerRack) {
    this.minSplitSizePerRack = minSplitSizePerRack;
  }

  public String getInputformat() {
    return inputformat;
  }

  public void setInputformat(String inputformat) {
    this.inputformat = inputformat;
  }
  public boolean isUseBucketizedHiveInputFormat() {
    return useBucketizedHiveInputFormat;
  }

  public void setUseBucketizedHiveInputFormat(boolean useBucketizedHiveInputFormat) {
    this.useBucketizedHiveInputFormat = useBucketizedHiveInputFormat;
  }

  public QBJoinTree getJoinTree() {
    return joinTree;
  }

  public void setJoinTree(QBJoinTree joinTree) {
    this.joinTree = joinTree;
  }

  public void setMapperCannotSpanPartns(boolean mapperCannotSpanPartns) {
    this.mapperCannotSpanPartns = mapperCannotSpanPartns;
  }

  public boolean isMapperCannotSpanPartns() {
    return this.mapperCannotSpanPartns;
  }

  public boolean getHadoopSupportsSplittable() {
    return hadoopSupportsSplittable;
  }

  public void setHadoopSupportsSplittable(boolean hadoopSupportsSplittable) {
    this.hadoopSupportsSplittable = hadoopSupportsSplittable;
  }

  public String getIndexIntermediateFile() {
    return indexIntermediateFile;
  }

  public ArrayList<String> getAliases() {
    return new ArrayList<String>(aliasToWork.keySet());
  }

  public ArrayList<Operator<?>> getWorks() {
    return new ArrayList<Operator<?>>(aliasToWork.values());
  }

  public ArrayList<String> getPaths() {
    return new ArrayList<String>(pathToAliases.keySet());
  }

  public ArrayList<PartitionDesc> getPartitionDescs() {
    return new ArrayList<PartitionDesc>(aliasToPartnInfo.values());
  }

  public
    LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> getOpParseCtxMap() {
    return opParseCtxMap;
  }

  public void setOpParseCtxMap(
    LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtxMap) {
    this.opParseCtxMap = opParseCtxMap;
  }

  public String getTmpHDFSFileURI() {
    return tmpHDFSFileURI;
  }

  public void setTmpHDFSFileURI(String tmpHDFSFileURI) {
    this.tmpHDFSFileURI = tmpHDFSFileURI;
  }

  public void mergingInto(MapWork mapWork) {
    // currently, this is sole field affecting mergee task
    mapWork.useBucketizedHiveInputFormat |= useBucketizedHiveInputFormat;
  }

  @Explain(displayName = "Path -> Bucketed Columns", normalExplain = false)
  public Map<String, List<BucketCol>> getBucketedColsByDirectory() {
    return bucketedColsByDirectory;
  }

  @Explain(displayName = "Path -> Sorted Columns", normalExplain = false)
  public Map<String, List<SortCol>> getSortedColsByDirectory() {
    return sortedColsByDirectory;
  }

  public void addIndexIntermediateFile(String fileName) {
    if (this.indexIntermediateFile == null) {
      this.indexIntermediateFile = fileName;
    } else {
      this.indexIntermediateFile += "," + fileName;
    }
  }

  public int getSamplingType() {
    return samplingType;
  }

  public void setSamplingType(int samplingType) {
    this.samplingType = samplingType;
  }

  @Explain(displayName = "Sampling")
  public String getSamplingTypeString() {
    return samplingType == 1 ? "SAMPLING_ON_PREV_MR" :
        samplingType == 2 ? "SAMPLING_ON_START" : null;
  }

  public void configureJobConf(JobConf job) {
    for (PartitionDesc partition : aliasToPartnInfo.values()) {
      PlanUtils.configureJobConf(partition.getTableDesc(), job);
    }
    Collection<Operator<?>> mappers = aliasToWork.values();
    for (FileSinkOperator fs : OperatorUtils.findOperators(mappers, FileSinkOperator.class)) {
      PlanUtils.configureJobConf(fs.getConf().getTableInfo(), job);
    }
  }
}
