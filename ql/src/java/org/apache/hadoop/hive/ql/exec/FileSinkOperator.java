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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.io.FSRecordWriter;
import org.apache.hadoop.hive.ql.io.FSRecordWriter.StatsProvidingRecordWriter;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HivePartitioner;
import org.apache.hadoop.hive.ql.io.HivePassThroughOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveFatalException;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc.DPSortState;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.SkewedColumnPositionPair;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.stats.StatsCollectionTaskIndependent;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SubStructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.Lists;

/**
 * File Sink operator implementation.
 **/
public class FileSinkOperator extends TerminalOperator<FileSinkDesc> implements
    Serializable {

  protected transient HashMap<String, FSPaths> valToPaths;
  protected transient int numDynParts;
  protected transient List<String> dpColNames;
  protected transient DynamicPartitionCtx dpCtx;
  protected transient boolean isCompressed;
  protected transient Path parent;
  protected transient HiveOutputFormat<?, ?> hiveOutputFormat;
  protected transient Path specPath;
  protected transient String childSpecPathDynLinkedPartitions;
  protected transient int dpStartCol; // start column # for DP columns
  protected transient List<String> dpVals; // array of values corresponding to DP columns
  protected transient List<Object> dpWritables;
  protected transient FSRecordWriter[] rowOutWriters; // row specific RecordWriters
  protected transient int maxPartitions;
  protected transient ListBucketingCtx lbCtx;
  protected transient boolean isSkewedStoredAsSubDirectories;
  protected transient boolean statsCollectRawDataSize;
  private transient boolean[] statsFromRecordWriter;
  private transient boolean isCollectRWStats;
  private transient FSPaths prevFsp;
  private transient FSPaths fpaths;
  private transient ObjectInspector keyOI;
  private transient List<Object> keyWritables;
  private transient List<String> keys;
  private transient int numKeyColToRead;

  /**
   * RecordWriter.
   *
   */
  public static interface RecordWriter {
    void write(Writable w) throws IOException;

    void close(boolean abort) throws IOException;
  }

  public class FSPaths implements Cloneable {
    Path tmpPath;
    Path taskOutputTempPath;
    Path[] outPaths;
    Path[] finalPaths;
    FSRecordWriter[] outWriters;
    Stat stat;

    public FSPaths() {
    }

    public FSPaths(Path specPath) {
      tmpPath = Utilities.toTempPath(specPath);
      taskOutputTempPath = Utilities.toTaskTempPath(specPath);
      outPaths = new Path[numFiles];
      finalPaths = new Path[numFiles];
      outWriters = new FSRecordWriter[numFiles];
      stat = new Stat();
    }

    /**
     * Update OutPath according to tmpPath.
     */
    public Path getTaskOutPath(String taskId) {
      return new Path(this.taskOutputTempPath, Utilities.toTempPath(taskId));
    }

    /**
     * Update the final paths according to tmpPath.
     */
    public Path getFinalPath(String taskId, Path tmpPath, String extension) {
      if (extension != null) {
        return new Path(tmpPath, taskId + extension);
      } else {
        return new Path(tmpPath, taskId);
      }
    }

    public void setOutWriters(FSRecordWriter[] out) {
      outWriters = out;
    }

    public FSRecordWriter[] getOutWriters() {
      return outWriters;
    }

    public void closeWriters(boolean abort) throws HiveException {
      for (int idx = 0; idx < outWriters.length; idx++) {
        if (outWriters[idx] != null) {
          try {
            outWriters[idx].close(abort);
            updateProgress();
          } catch (IOException e) {
            throw new HiveException(e);
          }
        }
      }
    }

    private void commit(FileSystem fs) throws HiveException {
      for (int idx = 0; idx < outPaths.length; ++idx) {
        try {
          if ((bDynParts || isSkewedStoredAsSubDirectories)
              && !fs.exists(finalPaths[idx].getParent())) {
            fs.mkdirs(finalPaths[idx].getParent());
          }
          if (!fs.rename(outPaths[idx], finalPaths[idx])) {
            throw new HiveException("Unable to rename output from: " +
                outPaths[idx] + " to: " + finalPaths[idx]);
          }
          updateProgress();
        } catch (IOException e) {
          throw new HiveException("Unable to rename output from: " +
              outPaths[idx] + " to: " + finalPaths[idx], e);
        }
      }
    }

    public void abortWriters(FileSystem fs, boolean abort, boolean delete) throws HiveException {
      for (int idx = 0; idx < outWriters.length; idx++) {
        if (outWriters[idx] != null) {
          try {
            outWriters[idx].close(abort);
            if (delete) {
              fs.delete(outPaths[idx], true);
            }
            updateProgress();
          } catch (IOException e) {
            throw new HiveException(e);
          }
        }
      }
    }

    public Stat getStat() {
      return stat;
    }
  } // class FSPaths

  private static final long serialVersionUID = 1L;
  protected transient FileSystem fs;
  protected transient Serializer serializer;
  protected transient LongWritable row_count;
  private transient boolean isNativeTable = true;

  /**
   * The evaluators for the multiFile sprayer. If the table under consideration has 1000 buckets,
   * it is not a good idea to start so many reducers - if the maximum number of reducers is 100,
   * each reducer can write 10 files - this way we effectively get 1000 files.
   */
  private transient ExprNodeEvaluator[] partitionEval;
  protected transient int totalFiles;
  private transient int numFiles;
  protected transient boolean multiFileSpray;
  protected transient final Map<Integer, Integer> bucketMap = new HashMap<Integer, Integer>();

  private transient ObjectInspector[] partitionObjectInspectors;
  protected transient HivePartitioner<HiveKey, Object> prtner;
  protected transient final HiveKey key = new HiveKey();
  private transient Configuration hconf;
  protected transient FSPaths fsp;
  protected transient boolean bDynParts;
  private transient SubStructObjectInspector subSetOI;
  private transient int timeOut; // JT timeout in msec.
  private transient long lastProgressReport = System.currentTimeMillis();

  protected transient boolean autoDelete = false;
  protected transient JobConf jc;
  Class<? extends Writable> outputClass;
  String taskId;

  protected boolean filesCreated = false;

  private void initializeSpecPath() {
    // For a query of the type:
    // insert overwrite table T1
    // select * from (subq1 union all subq2)u;
    // subQ1 and subQ2 write to directories Parent/Child_1 and
    // Parent/Child_2 respectively, and union is removed.
    // The movetask that follows subQ1 and subQ2 tasks moves the directory
    // 'Parent'

    // However, if the above query contains dynamic partitions, subQ1 and
    // subQ2 have to write to directories: Parent/DynamicPartition/Child_1
    // and Parent/DynamicPartition/Child_1 respectively.
    // The movetask that follows subQ1 and subQ2 tasks still moves the directory
    // 'Parent'
    if ((!conf.isLinkedFileSink()) || (dpCtx == null)) {
      specPath = conf.getDirName();
      childSpecPathDynLinkedPartitions = null;
      return;
    }

    specPath = conf.getParentDir();
    childSpecPathDynLinkedPartitions = conf.getDirName().getName();
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    try {
      this.hconf = hconf;
      filesCreated = false;
      isNativeTable = !conf.getTableInfo().isNonNative();
      multiFileSpray = conf.isMultiFileSpray();
      totalFiles = conf.getTotalFiles();
      numFiles = conf.getNumFiles();
      dpCtx = conf.getDynPartCtx();
      lbCtx = conf.getLbCtx();
      valToPaths = new HashMap<String, FSPaths>();
      taskId = Utilities.getTaskId(hconf);
      initializeSpecPath();
      fs = specPath.getFileSystem(hconf);
      hiveOutputFormat = conf.getTableInfo().getOutputFileFormatClass().newInstance();
      isCompressed = conf.getCompressed();
      parent = Utilities.toTempPath(conf.getDirName());
      statsCollectRawDataSize = conf.isStatsCollectRawDataSize();
      statsFromRecordWriter = new boolean[numFiles];

      serializer = (Serializer) conf.getTableInfo().getDeserializerClass().newInstance();
      serializer.initialize(null, conf.getTableInfo().getProperties());
      outputClass = serializer.getSerializedClass();

      // Timeout is chosen to make sure that even if one iteration takes more than
      // half of the script.timeout but less than script.timeout, we will still
      // be able to report progress.
      timeOut = hconf.getInt("mapred.healthChecker.script.timeout", 600000) / 2;

      if (hconf instanceof JobConf) {
        jc = (JobConf) hconf;
      } else {
        // test code path
        jc = new JobConf(hconf);
      }

      if (multiFileSpray) {
        partitionEval = new ExprNodeEvaluator[conf.getPartitionCols().size()];
        int i = 0;
        for (ExprNodeDesc e : conf.getPartitionCols()) {
          partitionEval[i++] = ExprNodeEvaluatorFactory.get(e);
        }

        partitionObjectInspectors = initEvaluators(partitionEval, outputObjInspector);
        prtner = (HivePartitioner<HiveKey, Object>) ReflectionUtils.newInstance(
            jc.getPartitionerClass(), null);
      }
      row_count = new LongWritable();
      if (dpCtx != null) {
        dpSetup();
      }

      if (lbCtx != null) {
        lbSetup();
      }

      int numPart = 0;
      int numBuck = 0;
      if (conf.getPartitionCols() != null && !conf.getPartitionCols().isEmpty()) {
        numPart = conf.getPartitionCols().size();
      }

      // bucket number will exists only in PARTITION_BUCKET_SORTED mode
      if (conf.getDpSortState().equals(DPSortState.PARTITION_BUCKET_SORTED)) {
        numBuck = 1;
      }
      numKeyColToRead = numPart + numBuck;
      keys = Lists.newArrayListWithCapacity(numKeyColToRead);
      keyWritables = Lists.newArrayListWithCapacity(numKeyColToRead);

      if (!bDynParts) {
        fsp = new FSPaths(specPath);

        // Create all the files - this is required because empty files need to be created for
        // empty buckets
        // createBucketFiles(fsp);
        if (!this.isSkewedStoredAsSubDirectories) {
          valToPaths.put("", fsp); // special entry for non-DP case
        }
      }
      initializeChildren(hconf);
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  /**
   * Initialize list bucketing information
   */
  private void lbSetup() {
    this.isSkewedStoredAsSubDirectories =  ((lbCtx == null) ? false : lbCtx.isSkewedStoredAsDir());
  }

  /**
   * Set up for dynamic partitioning including a new ObjectInspector for the output row.
   */
  private void dpSetup() {

    this.bDynParts = false;
    this.numDynParts = dpCtx.getNumDPCols();
    this.dpColNames = dpCtx.getDPColNames();
    this.maxPartitions = dpCtx.getMaxPartitionsPerNode();

    assert numDynParts == dpColNames.size() : "number of dynamic paritions should be the same as the size of DP mapping";

    if (dpColNames != null && dpColNames.size() > 0) {
      this.bDynParts = true;
      assert inputObjInspectors.length == 1 : "FileSinkOperator should have 1 parent, but it has "
          + inputObjInspectors.length;
      StructObjectInspector soi = (StructObjectInspector) inputObjInspectors[0];
      // remove the last dpMapping.size() columns from the OI
      List<? extends StructField> fieldOI = soi.getAllStructFieldRefs();
      ArrayList<ObjectInspector> newFieldsOI = new ArrayList<ObjectInspector>();
      ArrayList<String> newFieldsName = new ArrayList<String>();
      this.dpStartCol = 0;
      for (StructField sf : fieldOI) {
        String fn = sf.getFieldName();
        if (!dpCtx.getInputToDPCols().containsKey(fn)) {
          newFieldsOI.add(sf.getFieldObjectInspector());
          newFieldsName.add(sf.getFieldName());
          this.dpStartCol++;
        }
      }
      assert newFieldsOI.size() > 0 : "new Fields ObjectInspector is empty";

      this.subSetOI = new SubStructObjectInspector(soi, 0, this.dpStartCol);
      this.dpVals = new ArrayList<String>(numDynParts);
      this.dpWritables = new ArrayList<Object>(numDynParts);
    }
  }

  protected void createBucketFiles(FSPaths fsp) throws HiveException {
    try {
      int filesIdx = 0;
      Set<Integer> seenBuckets = new HashSet<Integer>();
      for (int idx = 0; idx < totalFiles; idx++) {
        if (this.getExecContext() != null && this.getExecContext().getFileId() != null) {
          LOG.info("replace taskId from execContext ");

          taskId = Utilities.replaceTaskIdFromFilename(taskId, this.getExecContext().getFileId());

          LOG.info("new taskId: FS " + taskId);

          assert !multiFileSpray;
          assert totalFiles == 1;
        }

        if (multiFileSpray) {
          key.setHashCode(idx);

          // Does this hashcode belong to this reducer
          int numReducers = totalFiles / numFiles;

          if (numReducers > 1) {
            int currReducer = Integer.valueOf(Utilities.getTaskIdFromFilename(Utilities
                .getTaskId(hconf)));

            int reducerIdx = prtner.getPartition(key, null, numReducers);
            if (currReducer != reducerIdx) {
              continue;
            }
          }

          int bucketNum = prtner.getBucket(key, null, totalFiles);
          if (seenBuckets.contains(bucketNum)) {
            continue;
          }
          seenBuckets.add(bucketNum);

          bucketMap.put(bucketNum, filesIdx);
          taskId = Utilities.replaceTaskIdFromFilename(Utilities.getTaskId(hconf), bucketNum);
        }
        createBucketForFileIdx(fsp, filesIdx);
        filesIdx++;
      }
      assert filesIdx == numFiles;

      // in recent hadoop versions, use deleteOnExit to clean tmp files.
      if (isNativeTable) {
        autoDelete = fs.deleteOnExit(fsp.outPaths[0]);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }

    filesCreated = true;
  }

  protected void createBucketForFileIdx(FSPaths fsp, int filesIdx) throws HiveException {
    try {
      if (isNativeTable) {
        fsp.finalPaths[filesIdx] = fsp.getFinalPath(taskId, fsp.tmpPath, null);
        LOG.info("Final Path: FS " + fsp.finalPaths[filesIdx]);
        fsp.outPaths[filesIdx] = fsp.getTaskOutPath(taskId);
        LOG.info("Writing to temp file: FS " + fsp.outPaths[filesIdx]);
      } else {
        fsp.finalPaths[filesIdx] = fsp.outPaths[filesIdx] = specPath;
      }
      // The reason to keep these instead of using
      // OutputFormat.getRecordWriter() is that
      // getRecordWriter does not give us enough control over the file name that
      // we create.
      String extension = Utilities.getFileExtension(jc, isCompressed, hiveOutputFormat);
      if (!bDynParts && !this.isSkewedStoredAsSubDirectories) {
        fsp.finalPaths[filesIdx] = fsp.getFinalPath(taskId, parent, extension);
      } else {
        fsp.finalPaths[filesIdx] = fsp.getFinalPath(taskId, fsp.tmpPath, extension);
      }

      LOG.info("New Final Path: FS " + fsp.finalPaths[filesIdx]);

      if (isNativeTable) {
        // in recent hadoop versions, use deleteOnExit to clean tmp files.
        autoDelete = fs.deleteOnExit(fsp.outPaths[filesIdx]);
      }

      Utilities.copyTableJobPropertiesToConf(conf.getTableInfo(), jc);
      // only create bucket files only if no dynamic partitions,
      // buckets of dynamic partitions will be created for each newly created partition
      fsp.outWriters[filesIdx] = HiveFileFormatUtils.getHiveRecordWriter(jc, conf.getTableInfo(),
          outputClass, conf, fsp.outPaths[filesIdx], reporter);
      // If the record writer provides stats, get it from there instead of the serde
      statsFromRecordWriter[filesIdx] = fsp.outWriters[filesIdx] instanceof StatsProvidingRecordWriter;
      // increment the CREATED_FILES counter
      if (reporter != null) {
        reporter.incrCounter(HiveConf.getVar(hconf, HiveConf.ConfVars.HIVECOUNTERGROUP),
            Operator.HIVECOUNTERCREATEDFILES, 1);
      }

    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Report status to JT so that JT won't kill this task if closing takes too long
   * due to too many files to close and the NN is overloaded.
   *
   * @return true if a new progress update is reported, false otherwise.
   */
  protected boolean updateProgress() {
    if (reporter != null &&
        (System.currentTimeMillis() - lastProgressReport) > timeOut) {
      reporter.progress();
      lastProgressReport = System.currentTimeMillis();
      return true;
    } else {
      return false;
    }
  }

  protected Writable recordValue;

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    /* Create list bucketing sub-directory only if stored-as-directories is on. */
    String lbDirName = null;
    lbDirName = (lbCtx == null) ? null : generateListBucketingDirName(row);

    if (!bDynParts && !filesCreated) {
      if (lbDirName != null) {
        FSPaths fsp2 = lookupListBucketingPaths(lbDirName);
      } else {
        createBucketFiles(fsp);
      }
    }

    try {
      updateProgress();

      // if DP is enabled, get the final output writers and prepare the real output row
      assert inputObjInspectors[0].getCategory() == ObjectInspector.Category.STRUCT : "input object inspector is not struct";

      if (bDynParts) {
        // copy the DP column values from the input row to dpVals
        dpVals.clear();
        dpWritables.clear();
        ObjectInspectorUtils.partialCopyToStandardObject(dpWritables, row, dpStartCol, numDynParts,
            (StructObjectInspector) inputObjInspectors[0], ObjectInspectorCopyOption.WRITABLE);
        // get a set of RecordWriter based on the DP column values
        // pass the null value along to the escaping process to determine what the dir should be
        for (Object o : dpWritables) {
          if (o == null || o.toString().length() == 0) {
            dpVals.add(dpCtx.getDefaultPartitionName());
          } else {
            dpVals.add(o.toString());
          }
        }
        // use SubStructObjectInspector to serialize the non-partitioning columns in the input row
        recordValue = serializer.serialize(row, subSetOI);

        // when dynamic partition sorting is not used, the DPSortState will be NONE
        // in which we will fall back to old method of file system path creation
        // i.e, having as many record writers as distinct values in partition column
        if (conf.getDpSortState().equals(DPSortState.NONE)) {
          fpaths = getDynOutPaths(dpVals, lbDirName);
        }

      } else {
        if (lbDirName != null) {
          fpaths = lookupListBucketingPaths(lbDirName);
        } else {
          fpaths = fsp;
        }
        // use SerDe to serialize r, and write it out
        recordValue = serializer.serialize(row, inputObjInspectors[0]);
      }

      rowOutWriters = fpaths.outWriters;
      // check if all record writers implement statistics. if atleast one RW
      // doesn't implement stats interface we will fallback to conventional way
      // of gathering stats
      isCollectRWStats = areAllTrue(statsFromRecordWriter);
      if (conf.isGatherStats() && !isCollectRWStats) {
        if (statsCollectRawDataSize) {
          SerDeStats stats = serializer.getSerDeStats();
          if (stats != null) {
            fpaths.stat.addToStat(StatsSetupConst.RAW_DATA_SIZE, stats.getRawDataSize());
          }
        }
        fpaths.stat.addToStat(StatsSetupConst.ROW_COUNT, 1);
      }


      FSRecordWriter rowOutWriter = null;

      if (row_count != null) {
        row_count.set(row_count.get() + 1);
      }

      if (!multiFileSpray) {
        rowOutWriter = rowOutWriters[0];
      } else {
        int keyHashCode = 0;
        for (int i = 0; i < partitionEval.length; i++) {
          Object o = partitionEval[i].evaluate(row);
          keyHashCode = keyHashCode * 31
              + ObjectInspectorUtils.hashCode(o, partitionObjectInspectors[i]);
        }
        key.setHashCode(keyHashCode);
        int bucketNum = prtner.getBucket(key, null, totalFiles);
        int idx = bucketMap.get(bucketNum);
        rowOutWriter = rowOutWriters[idx];
      }
      rowOutWriter.write(recordValue);
    } catch (IOException e) {
      throw new HiveException(e);
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
  }

  private boolean areAllTrue(boolean[] statsFromRW) {
    for(boolean b : statsFromRW) {
      if (!b) {
        return false;
      }
    }
    return true;
  }

  /**
   * Lookup list bucketing path.
   * @param lbDirName
   * @return
   * @throws HiveException
   */
  protected FSPaths lookupListBucketingPaths(String lbDirName) throws HiveException {
    FSPaths fsp2 = valToPaths.get(lbDirName);
    if (fsp2 == null) {
      fsp2 = createNewPaths(lbDirName);
    }
    return fsp2;
  }

  /**
   * create new path.
   *
   * @param dirName
   * @return
   * @throws HiveException
   */
  private FSPaths createNewPaths(String dirName) throws HiveException {
    FSPaths fsp2 = new FSPaths(specPath);
    if (childSpecPathDynLinkedPartitions != null) {
      fsp2.tmpPath = new Path(fsp2.tmpPath,
          dirName + Path.SEPARATOR + childSpecPathDynLinkedPartitions);
      fsp2.taskOutputTempPath =
        new Path(fsp2.taskOutputTempPath,
            dirName + Path.SEPARATOR + childSpecPathDynLinkedPartitions);
    } else {
      fsp2.tmpPath = new Path(fsp2.tmpPath, dirName);
      fsp2.taskOutputTempPath =
        new Path(fsp2.taskOutputTempPath, dirName);
    }
    if(!conf.getDpSortState().equals(DPSortState.PARTITION_BUCKET_SORTED)) {
      createBucketFiles(fsp2);
      valToPaths.put(dirName, fsp2);
    }
    return fsp2;
  }

  /**
   * Generate list bucketing directory name from a row.
   * @param row row to process.
   * @return directory name.
   */
  protected String generateListBucketingDirName(Object row) {
    if (!this.isSkewedStoredAsSubDirectories) {
      return null;
    }

    String lbDirName = null;
    List<Object> standObjs = new ArrayList<Object>();
    List<String> skewedCols = lbCtx.getSkewedColNames();
    List<List<String>> allSkewedVals = lbCtx.getSkewedColValues();
    List<String> skewedValsCandidate = null;
    Map<List<String>, String> locationMap = lbCtx.getLbLocationMap();

    /* Convert input row to standard objects. */
    ObjectInspectorUtils.copyToStandardObject(standObjs, row,
        (StructObjectInspector) inputObjInspectors[0], ObjectInspectorCopyOption.WRITABLE);

    assert (standObjs.size() >= skewedCols.size()) :
      "The row has less number of columns than no. of skewed column.";

    skewedValsCandidate = new ArrayList<String>(skewedCols.size());
    for (SkewedColumnPositionPair posPair : lbCtx.getRowSkewedIndex()) {
      skewedValsCandidate.add(posPair.getSkewColPosition(),
          standObjs.get(posPair.getTblColPosition()).toString());
    }
    /* The row matches skewed column names. */
    if (allSkewedVals.contains(skewedValsCandidate)) {
      /* matches skewed values. */
      lbDirName = FileUtils.makeListBucketingDirName(skewedCols, skewedValsCandidate);
      locationMap.put(skewedValsCandidate, lbDirName);
    } else {
      /* create default directory. */
      lbDirName = FileUtils.makeDefaultListBucketingDirName(skewedCols,
          lbCtx.getDefaultDirName());
      List<String> defaultKey = Arrays.asList(lbCtx.getDefaultKey());
      if (!locationMap.containsKey(defaultKey)) {
        locationMap.put(defaultKey, lbDirName);
      }
    }
    return lbDirName;
  }

  protected FSPaths getDynOutPaths(List<String> row, String lbDirName) throws HiveException {

    FSPaths fp;

    // get the path corresponding to the dynamic partition columns,
    String dpDir = getDynPartDirectory(row, dpColNames, numDynParts);

    String pathKey = null;
    if (dpDir != null) {
      dpDir = appendToSource(lbDirName, dpDir);
      pathKey = dpDir;
      if(conf.getDpSortState().equals(DPSortState.PARTITION_BUCKET_SORTED)) {
        String buckNum = row.get(row.size() - 1);
        taskId = Utilities.replaceTaskIdFromFilename(Utilities.getTaskId(hconf), buckNum);
        pathKey = appendToSource(taskId, dpDir);
      }
      FSPaths fsp2 = valToPaths.get(pathKey);

      if (fsp2 == null) {
        // check # of dp
        if (valToPaths.size() > maxPartitions) {
          // we cannot proceed and need to tell the hive client that retries won't succeed either
          throw new HiveFatalException(
               ErrorMsg.DYNAMIC_PARTITIONS_TOO_MANY_PER_NODE_ERROR.getErrorCodedMsg()
               + "Maximum was set to: " + maxPartitions);
        }

        if (!conf.getDpSortState().equals(DPSortState.NONE) && prevFsp != null) {
          // close the previous fsp as it is no longer needed
          prevFsp.closeWriters(false);

          // since we are closing the previous fsp's record writers, we need to see if we can get
          // stats from the record writer and store in the previous fsp that is cached
          if (conf.isGatherStats() && isCollectRWStats) {
            FSRecordWriter outWriter = prevFsp.outWriters[0];
            if (outWriter != null) {
              SerDeStats stats = ((StatsProvidingRecordWriter) outWriter).getStats();
              if (stats != null) {
                prevFsp.stat.addToStat(StatsSetupConst.RAW_DATA_SIZE, stats.getRawDataSize());
                prevFsp.stat.addToStat(StatsSetupConst.ROW_COUNT, stats.getRowCount());
              }
            }
          }

          // let writers release the memory for garbage collection
          prevFsp.outWriters[0] = null;

          prevFsp = null;
        }

        fsp2 = createNewPaths(dpDir);
        if (prevFsp == null) {
          prevFsp = fsp2;
        }

        if(conf.getDpSortState().equals(DPSortState.PARTITION_BUCKET_SORTED)) {
          createBucketForFileIdx(fsp2, 0);
          valToPaths.put(pathKey, fsp2);
        }
      }
      fp = fsp2;
    } else {
      fp = fsp;
    }
    return fp;
  }

  /**
   * Append dir to source dir
   * @param appendDir
   * @param srcDir
   * @return
   */
  private String appendToSource(String appendDir, String srcDir) {
    StringBuilder builder = new StringBuilder(srcDir);
    srcDir = (appendDir == null) ? srcDir : builder.append(Path.SEPARATOR).append(appendDir)
          .toString();
    return srcDir;
  }

  // given the current input row, the mapping for input col info to dp columns, and # of dp cols,
  // return the relative path corresponding to the row.
  // e.g., ds=2008-04-08/hr=11
  private String getDynPartDirectory(List<String> row, List<String> dpColNames, int numDynParts) {
    assert row.size() == numDynParts && numDynParts == dpColNames.size() : "data length is different from num of DP columns";
    return FileUtils.makePartName(dpColNames, row);
  }

  @Override
  public void startGroup() throws HiveException {
    if (!conf.getDpSortState().equals(DPSortState.NONE)) {
      keyOI = getGroupKeyObjectInspector();
      keys.clear();
      keyWritables.clear();
      ObjectInspectorUtils.partialCopyToStandardObject(keyWritables, getGroupKeyObject(), 0,
          numKeyColToRead, (StructObjectInspector) keyOI, ObjectInspectorCopyOption.WRITABLE);

      for (Object o : keyWritables) {
        if (o == null || o.toString().length() == 0) {
          keys.add(dpCtx.getDefaultPartitionName());
        } else {
          keys.add(o.toString());
        }
      }
      fpaths = getDynOutPaths(keys, null);
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!bDynParts && !filesCreated) {
      createBucketFiles(fsp);
    }

    lastProgressReport = System.currentTimeMillis();
    if (!abort) {
      for (FSPaths fsp : valToPaths.values()) {
        fsp.closeWriters(abort);

        // before closing the operator check if statistics gathering is requested
        // and is provided by record writer. this is different from the statistics
        // gathering done in processOp(). In processOp(), for each row added
        // serde statistics about the row is gathered and accumulated in hashmap.
        // this adds more overhead to the actual processing of row. But if the
        // record writer already gathers the statistics, it can simply return the
        // accumulated statistics which will be aggregated in case of spray writers
        if (conf.isGatherStats() && isCollectRWStats) {
          for (int idx = 0; idx < fsp.outWriters.length; idx++) {
            FSRecordWriter outWriter = fsp.outWriters[idx];
            if (outWriter != null) {
              SerDeStats stats = ((StatsProvidingRecordWriter) outWriter).getStats();
              if (stats != null) {
                fsp.stat.addToStat(StatsSetupConst.RAW_DATA_SIZE, stats.getRawDataSize());
                fsp.stat.addToStat(StatsSetupConst.ROW_COUNT, stats.getRowCount());
              }
            }
          }
        }

        if (isNativeTable) {
          fsp.commit(fs);
        }
      }
      // Only publish stats if this operator's flag was set to gather stats
      if (conf.isGatherStats()) {
        publishStats();
      }
    } else {
      // Will come here if an Exception was thrown in map() or reduce().
      // Hadoop always call close() even if an Exception was thrown in map() or
      // reduce().
      for (FSPaths fsp : valToPaths.values()) {
        fsp.abortWriters(fs, abort, !autoDelete && isNativeTable);
      }
    }
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "FS";
  }

  @Override
  public void jobCloseOp(Configuration hconf, boolean success)
      throws HiveException {
    try {
      if ((conf != null) && isNativeTable) {
        Path specPath = conf.getDirName();
        DynamicPartitionCtx dpCtx = conf.getDynPartCtx();
        if (conf.isLinkedFileSink() && (dpCtx != null)) {
          specPath = conf.getParentDir();
        }
        Utilities.mvFileToFinalPath(specPath, hconf, success, LOG, dpCtx, conf,
          reporter);
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
    super.jobCloseOp(hconf, success);
  }

  @Override
  public OperatorType getType() {
    return OperatorType.FILESINK;
  }

  @Override
  public void augmentPlan() {
    PlanUtils.configureOutputJobPropertiesForStorageHandler(
        getConf().getTableInfo());
  }

  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    if (hiveOutputFormat == null) {
      try {
        if (getConf().getTableInfo().getJobProperties() != null) {
             //Setting only for Storage Handler
             if (getConf().getTableInfo().getJobProperties().get(HivePassThroughOutputFormat.HIVE_PASSTHROUGH_STORAGEHANDLER_OF_JOBCONFKEY) != null) {
                 job.set(HivePassThroughOutputFormat.HIVE_PASSTHROUGH_STORAGEHANDLER_OF_JOBCONFKEY,getConf().getTableInfo().getJobProperties().get(HivePassThroughOutputFormat.HIVE_PASSTHROUGH_STORAGEHANDLER_OF_JOBCONFKEY));
                 hiveOutputFormat = ReflectionUtils.newInstance(conf.getTableInfo().getOutputFileFormatClass(),job);
           }
          else {
                 hiveOutputFormat = conf.getTableInfo().getOutputFileFormatClass().newInstance();
          }
        }
        else {
              hiveOutputFormat = conf.getTableInfo().getOutputFileFormatClass().newInstance();
        }
      } catch (Exception ex) {
        throw new IOException(ex);
      }
    }
    Utilities.copyTableJobPropertiesToConf(conf.getTableInfo(), job);

    if (conf.getTableInfo().isNonNative()) {
      //check the ouput specs only if it is a storage handler (native tables's outputformats does
      //not set the job's output properties correctly)
      try {
        hiveOutputFormat.checkOutputSpecs(ignored, job);
      } catch (NoSuchMethodError e) {
        //For BC, ignore this for now, but leave a log message
        LOG.warn("HiveOutputFormat should implement checkOutputSpecs() method`");
      }
    }
  }

  private void publishStats() throws HiveException {
    boolean isStatsReliable = conf.isStatsReliable();

    // Initializing a stats publisher
    StatsPublisher statsPublisher = Utilities.getStatsPublisher(jc);

    if (statsPublisher == null) {
      // just return, stats gathering should not block the main query
      LOG.error("StatsPublishing error: StatsPublisher is not initialized.");
      if (isStatsReliable) {
        throw new HiveException(ErrorMsg.STATSPUBLISHER_NOT_OBTAINED.getErrorCodedMsg());
      }
      return;
    }

    if (!statsPublisher.connect(hconf)) {
      // just return, stats gathering should not block the main query
      LOG.error("StatsPublishing error: cannot connect to database");
      if (isStatsReliable) {
        throw new HiveException(ErrorMsg.STATSPUBLISHER_CONNECTION_ERROR.getErrorCodedMsg());
      }
      return;
    }

    String taskID = Utilities.getTaskIdFromFilename(Utilities.getTaskId(hconf));
    String spSpec = conf.getStaticSpec();

    int maxKeyLength = conf.getMaxStatsKeyPrefixLength();
    boolean taskIndependent = statsPublisher instanceof StatsCollectionTaskIndependent;

    for (Map.Entry<String, FSPaths> entry : valToPaths.entrySet()) {
      String fspKey = entry.getKey();     // DP/LB
      FSPaths fspValue = entry.getValue();

      // for bucketed tables, hive.optimize.sort.dynamic.partition optimization
      // adds the taskId to the fspKey.
      if (conf.getDpSortState().equals(DPSortState.PARTITION_BUCKET_SORTED)) {
        taskID = Utilities.getTaskIdFromFilename(fspKey);
        // if length of (prefix/ds=__HIVE_DEFAULT_PARTITION__/000000_0) is greater than max key prefix
        // and if (prefix/ds=10/000000_0) is less than max key prefix, then former will get hashed
        // to a smaller prefix (MD5hash/000000_0) and later will stored as such in staging stats table.
        // When stats gets aggregated in StatsTask only the keys that starts with "prefix" will be fetched.
        // Now that (prefix/ds=__HIVE_DEFAULT_PARTITION__) is hashed to a smaller prefix it will
        // not be retrieved from staging table and hence not aggregated. To avoid this issue
        // we will remove the taskId from the key which is redundant anyway.
        fspKey = fspKey.split(taskID)[0];
      }

      // split[0] = DP, split[1] = LB
      String[] split = splitKey(fspKey);
      String dpSpec = split[0];
      String lbSpec = split[1];

      String prefix;
      String postfix=null;
      if (taskIndependent) {
        // key = "database.table/SP/DP/"LB/
        prefix = conf.getTableInfo().getTableName();
      } else {
        // key = "prefix/SP/DP/"LB/taskID/
        prefix = conf.getStatsAggPrefix();
        postfix = Utilities.join(lbSpec, taskID);
      }
      prefix = Utilities.join(prefix, spSpec, dpSpec);
      prefix = Utilities.getHashedStatsPrefix(prefix, maxKeyLength);

      String key = Utilities.join(prefix, postfix);

      Map<String, String> statsToPublish = new HashMap<String, String>();
      for (String statType : fspValue.stat.getStoredStats()) {
        statsToPublish.put(statType, Long.toString(fspValue.stat.getStat(statType)));
      }
      if (!statsPublisher.publishStat(key, statsToPublish)) {
        // The original exception is lost.
        // Not changing the interface to maintain backward compatibility
        if (isStatsReliable) {
          throw new HiveException(ErrorMsg.STATSPUBLISHER_PUBLISHING_ERROR.getErrorCodedMsg());
        }
      }
    }
    if (!statsPublisher.closeConnection()) {
      // The original exception is lost.
      // Not changing the interface to maintain backward compatibility
      if (isStatsReliable) {
        throw new HiveException(ErrorMsg.STATSPUBLISHER_CLOSING_ERROR.getErrorCodedMsg());
      }
    }
  }

  /**
   * This is server side code to create key in order to save statistics to stats database.
   * Client side will read it via StatsTask.java aggregateStats().
   * Client side reads it via db query prefix which is based on partition spec.
   * Since store-as-subdir information is not part of partition spec, we have to
   * remove store-as-subdir information from variable "keyPrefix" calculation.
   * But we have to keep store-as-subdir information in variable "key" calculation
   * since each skewed value has a row in stats db and "key" is db key,
   * otherwise later value overwrites previous value.
   * Performance impact due to string handling is minimum since this method is
   * only called once in FileSinkOperator closeOp().
   * For example,
   * create table test skewed by (key, value) on (('484','val_484') stored as DIRECTORIES;
   * skewedValueDirList contains 2 elements:
   * 1. key=484/value=val_484
   * 2. HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME/HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME
   * Case #1: Static partition with store-as-sub-dir
   * spSpec has SP path
   * fspKey has either
   * key=484/value=val_484 or
   * HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME/HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME
   * After filter, fspKey is empty, storedAsDirPostFix has either
   * key=484/value=val_484 or
   * HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME/HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME
   * so, at the end, "keyPrefix" doesnt have subdir information but "key" has
   * Case #2: Dynamic partition with store-as-sub-dir. Assume dp part is hr
   * spSpec has SP path
   * fspKey has either
   * hr=11/key=484/value=val_484 or
   * hr=11/HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME/HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME
   * After filter, fspKey is hr=11, storedAsDirPostFix has either
   * key=484/value=val_484 or
   * HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME/HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME
   * so, at the end, "keyPrefix" doesn't have subdir information from skewed but "key" has
   *
   * In a word, fspKey is consists of DP(dynamic partition spec) + LB(list bucketing spec)
   * In stats publishing, full partition spec consists of prefix part of stat key
   * but list bucketing spec is regarded as a postfix of stat key. So we split it here.
   */
  private String[] splitKey(String fspKey) {
    if (!fspKey.isEmpty() && isSkewedStoredAsSubDirectories) {
      for (String dir : lbCtx.getSkewedValuesDirNames()) {
        int index = fspKey.indexOf(dir);
        if (index >= 0) {
          return new String[] {fspKey.substring(0, index), fspKey.substring(index + 1)};
        }
      }
    }
    return new String[] {fspKey, null};
  }
}
