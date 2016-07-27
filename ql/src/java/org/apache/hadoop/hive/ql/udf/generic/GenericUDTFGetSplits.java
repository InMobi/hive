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

package org.apache.hadoop.hive.ql.udf.generic;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.login.LoginException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.llap.LlapOutputFormat;
import org.apache.hadoop.hive.llap.SubmitWorkInfo;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.llap.Schema;
import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.TypeDesc;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.exec.tez.HiveSplitGenerator;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.TaskSpecBuilder;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.mapreduce.grouper.TezSplitGrouper;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * GenericUDTFGetSplits.
 *
 */
@Description(name = "get_splits", value = "_FUNC_(string,int) - "
    + "Returns an array of length int serialized splits for the referenced tables string.")
@UDFType(deterministic = false)
public class GenericUDTFGetSplits extends GenericUDTF {

  private static final Logger LOG = LoggerFactory.getLogger(GenericUDTFGetSplits.class);

  private static final String LLAP_INTERNAL_INPUT_FORMAT_NAME = "org.apache.hadoop.hive.llap.LlapInputFormat";

  protected transient StringObjectInspector stringOI;
  protected transient IntObjectInspector intOI;
  protected transient JobConf jc;
  private ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
  private DataOutput dos = new DataOutputStream(bos);

  @Override
  public StructObjectInspector initialize(ObjectInspector[] arguments)
    throws UDFArgumentException {

    LOG.debug("initializing GenericUDFGetSplits");

    if (SessionState.get() == null || SessionState.get().getConf() == null) {
      throw new IllegalStateException("Cannot run get splits outside HS2");
    }

    LOG.debug("Initialized conf, jc and metastore connection");

    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("The function GET_SPLITS accepts 2 arguments.");
    } else if (!(arguments[0] instanceof StringObjectInspector)) {
      LOG.error("Got "+arguments[0].getTypeName()+" instead of string.");
      throw new UDFArgumentTypeException(0, "\""
          + "string\" is expected at function GET_SPLITS, " + "but \""
          + arguments[0].getTypeName() + "\" is found");
    } else if (!(arguments[1] instanceof IntObjectInspector)) {
      LOG.error("Got "+arguments[1].getTypeName()+" instead of int.");
      throw new UDFArgumentTypeException(1, "\""
          + "int\" is expected at function GET_SPLITS, " + "but \""
          + arguments[1].getTypeName() + "\" is found");
    }

    stringOI = (StringObjectInspector) arguments[0];
    intOI = (IntObjectInspector) arguments[1];

    List<String> names = Arrays.asList("split");
    List<ObjectInspector> fieldOIs = Arrays.<ObjectInspector>asList(
      PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
    StructObjectInspector outputOI = ObjectInspectorFactory.getStandardStructObjectInspector(names, fieldOIs);

    LOG.debug("done initializing GenericUDFGetSplits");
    return outputOI;
  }

  public static class PlanFragment {
    public JobConf jc;
    public TezWork work;
    public Schema schema;

    public PlanFragment(TezWork work, Schema schema, JobConf jc) {
      this.work = work;
      this.schema = schema;
      this.jc = jc;
    }
  }

  @Override
  public void process(Object[] arguments) throws HiveException {

    String query = stringOI.getPrimitiveJavaObject(arguments[0]);
    int num = intOI.get(arguments[1]);

    PlanFragment fragment = createPlanFragment(query, num);
    TezWork tezWork = fragment.work;
    Schema schema = fragment.schema;

    try {
      for (InputSplit s: getSplits(jc, num, tezWork, schema)) {
        Object[] os = new Object[1];
        bos.reset();
        s.write(dos);
        byte[] frozen = bos.toByteArray();
        os[0] = frozen;
        forward(os);
      }
    } catch(Exception e) {
      throw new HiveException(e);
    }
  }

  public PlanFragment createPlanFragment(String query, int num) throws HiveException {

    HiveConf conf = new HiveConf(SessionState.get().getConf());
    HiveConf.setVar(conf, ConfVars.HIVEFETCHTASKCONVERSION, "none");
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT, "Llap");

    String originalMode = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_MODE);
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_MODE, "llap");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_TEZ_GENERATE_CONSISTENT_SPLITS, true);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.LLAP_CLIENT_CONSISTENT_SPLITS, true);
    conf.setBoolean(TezSplitGrouper.TEZ_GROUPING_NODE_LOCAL_ONLY, true);
    // Tez/LLAP requires RPC query plan
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN, true);

    try {
      jc = DagUtils.getInstance().createConfiguration(conf);
    } catch (IOException e) {
      throw new HiveException(e);
    }

    Driver driver = new Driver(conf);
    CommandProcessorResponse cpr;

    LOG.info("setting fetch.task.conversion to none and query file format to \""
        + LlapOutputFormat.class.getName()+"\"");

    cpr = driver.compileAndRespond(query);
    if(cpr.getResponseCode() != 0) {
      throw new HiveException("Failed to compile query: "+cpr.getException());
    }

    QueryPlan plan = driver.getPlan();
    List<Task<?>> roots = plan.getRootTasks();
    Schema schema = convertSchema(plan.getResultSchema());

    if (roots == null || roots.size() != 1 || !(roots.get(0) instanceof TezTask)) {
      throw new HiveException("Was expecting a single TezTask.");
    }

    TezWork tezWork = ((TezTask)roots.get(0)).getWork();

    if (tezWork.getAllWork().size() != 1) {

      String tableName = "table_"+UUID.randomUUID().toString().replaceAll("[^A-Za-z0-9 ]", "");

      String ctas = "create temporary table "+tableName+" as "+query;
      LOG.info("CTAS: "+ctas);

      try {
        HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_MODE, originalMode);
        cpr = driver.run(ctas, false);
      } catch(CommandNeedRetryException e) {
        throw new HiveException(e);
      }

      if(cpr.getResponseCode() != 0) {
        throw new HiveException("Failed to create temp table: " + cpr.getException());
      }

      HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_MODE, "llap");
      query = "select * from " + tableName;
      cpr = driver.compileAndRespond(query);
      if(cpr.getResponseCode() != 0) {
        throw new HiveException("Failed to create temp table: "+cpr.getException());
      }

      plan = driver.getPlan();
      roots = plan.getRootTasks();
      schema = convertSchema(plan.getResultSchema());

      if (roots == null || roots.size() != 1 || !(roots.get(0) instanceof TezTask)) {
        throw new HiveException("Was expecting a single TezTask.");
      }

      tezWork = ((TezTask)roots.get(0)).getWork();
    }

    return new PlanFragment(tezWork, schema, jc);
  }

  public InputSplit[] getSplits(JobConf job, int numSplits, TezWork work, Schema schema)
    throws IOException {

    DAG dag = DAG.create(work.getName());
    dag.setCredentials(job.getCredentials());

    DagUtils utils = DagUtils.getInstance();
    Context ctx = new Context(job);
    MapWork mapWork = (MapWork) work.getAllWork().get(0);
    // bunch of things get setup in the context based on conf but we need only the MR tmp directory
    // for the following method.
    JobConf wxConf = utils.initializeVertexConf(job, ctx, mapWork);
    Path scratchDir = utils.createTezDir(ctx.getMRScratchDir(), job);
    FileSystem fs = scratchDir.getFileSystem(job);
    try {
      LocalResource appJarLr = createJarLocalResource(utils.getExecJarPathLocal(), utils, job);
      Vertex wx = utils.createVertex(wxConf, mapWork, scratchDir, appJarLr,
          new ArrayList<LocalResource>(), fs, ctx, false, work,
          work.getVertexType(mapWork));
      String vertexName = wx.getName();
      dag.addVertex(wx);
      utils.addCredentials(mapWork, dag);


      // we have the dag now proceed to get the splits:
      Preconditions.checkState(HiveConf.getBoolVar(wxConf,
              HiveConf.ConfVars.HIVE_TEZ_GENERATE_CONSISTENT_SPLITS));
      Preconditions.checkState(HiveConf.getBoolVar(wxConf,
              HiveConf.ConfVars.LLAP_CLIENT_CONSISTENT_SPLITS));
      HiveSplitGenerator splitGenerator = new HiveSplitGenerator(wxConf, mapWork);
      List<Event> eventList = splitGenerator.initialize();

      InputSplit[] result = new InputSplit[eventList.size() - 1];
      DataOutputBuffer dob = new DataOutputBuffer();

      InputConfigureVertexTasksEvent configureEvent
        = (InputConfigureVertexTasksEvent) eventList.get(0);

      List<TaskLocationHint> hints = configureEvent.getLocationHint().getTaskLocationHints();

      Preconditions.checkState(hints.size() == eventList.size() - 1);

      if (LOG.isDebugEnabled()) {
        LOG.debug("NumEvents=" + eventList.size());
        LOG.debug("NumSplits=" + result.length);
      }

      ApplicationId fakeApplicationId
        = ApplicationId.newInstance(Math.abs(new Random().nextInt()), 0);

      String llapUser = UserGroupInformation.getLoginUser().getShortUserName();
      LOG.info("Number of splits: " + (eventList.size() - 1));
      for (int i = 0; i < eventList.size() - 1; i++) {

        TaskSpec taskSpec =
          new TaskSpecBuilder().constructTaskSpec(dag, vertexName,
              eventList.size() - 1, fakeApplicationId, i);

        SubmitWorkInfo submitWorkInfo =
          new SubmitWorkInfo(taskSpec, fakeApplicationId, System.currentTimeMillis());
        EventMetaData sourceMetaData =
          new EventMetaData(EventMetaData.EventProducerConsumerType.INPUT, vertexName,
              "NULL_VERTEX", null);
        EventMetaData destinationMetaInfo = new TaskSpecBuilder().getDestingationMetaData(wx);

        // Creating the TezEvent here itself, since it's easy to serialize.
        Event event = eventList.get(i + 1);
        TaskLocationHint hint = hints.get(i);
        Set<String> hosts = hint.getHosts();
        if (hosts.size() != 1) {
          LOG.warn("Bad # of locations: " + hosts.size());
        }
        SplitLocationInfo[] locations = new SplitLocationInfo[hosts.size()];

        int j = 0;
        for (String host : hosts) {
          locations[j++] = new SplitLocationInfo(host, false);
        }
        TezEvent tezEvent = new TezEvent(event, sourceMetaData, System.currentTimeMillis());
        tezEvent.setDestinationInfo(destinationMetaInfo);

        bos.reset();
        dob.reset();
        tezEvent.write(dob);

        byte[] submitWorkBytes = SubmitWorkInfo.toBytes(submitWorkInfo);

        result[i] = new LlapInputSplit(i, submitWorkBytes, dob.getData(), locations, schema, llapUser);
      }
      return result;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Returns a local resource representing a jar. This resource will be used to execute the plan on
   * the cluster.
   *
   * @param localJarPath
   *          Local path to the jar to be localized.
   * @return LocalResource corresponding to the localized hive exec resource.
   * @throws IOException
   *           when any file system related call fails.
   * @throws LoginException
   *           when we are unable to determine the user.
   * @throws URISyntaxException
   *           when current jar location cannot be determined.
   */
  private LocalResource createJarLocalResource(String localJarPath, DagUtils utils,
      Configuration conf)
    throws IOException, LoginException, IllegalArgumentException, FileNotFoundException {
    FileStatus destDirStatus = utils.getHiveJarDirectory(conf);
    assert destDirStatus != null;
    Path destDirPath = destDirStatus.getPath();

    Path localFile = new Path(localJarPath);
    String sha = getSha(localFile, conf);

    String destFileName = localFile.getName();

    // Now, try to find the file based on SHA and name. Currently we require exact name match.
    // We could also allow cutting off versions and other stuff provided that SHA matches...
    destFileName = FilenameUtils.removeExtension(destFileName) + "-" + sha
      + FilenameUtils.EXTENSION_SEPARATOR + FilenameUtils.getExtension(destFileName);

    // TODO: if this method is ever called on more than one jar, getting the dir and the
    // list need to be refactored out to be done only once.
    Path destFile = new Path(destDirPath.toString() + "/" + destFileName);
    return utils.localizeResource(localFile, destFile, LocalResourceType.FILE, conf);
  }

  private String getSha(Path localFile, Configuration conf)
    throws IOException, IllegalArgumentException {
    InputStream is = null;
    try {
      FileSystem localFs = FileSystem.getLocal(conf);
      is = localFs.open(localFile);
      return DigestUtils.sha256Hex(is);
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }

  private TypeDesc convertTypeString(String typeString) throws HiveException {
    TypeDesc typeDesc;
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeString);
    Preconditions.checkState(typeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE,
        "Unsupported non-primitive type " + typeString);

    switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
      case BOOLEAN:
        typeDesc = new TypeDesc(TypeDesc.Type.BOOLEAN);
        break;
      case BYTE:
        typeDesc = new TypeDesc(TypeDesc.Type.TINYINT);
        break;
      case SHORT:
        typeDesc = new TypeDesc(TypeDesc.Type.SMALLINT);
        break;
      case INT:
        typeDesc = new TypeDesc(TypeDesc.Type.INT);
        break;
      case LONG:
        typeDesc = new TypeDesc(TypeDesc.Type.BIGINT);
        break;
      case FLOAT:
        typeDesc = new TypeDesc(TypeDesc.Type.FLOAT);
        break;
      case DOUBLE:
        typeDesc = new TypeDesc(TypeDesc.Type.DOUBLE);
        break;
      case STRING:
        typeDesc = new TypeDesc(TypeDesc.Type.STRING);
        break;
      case CHAR:
        CharTypeInfo charTypeInfo = (CharTypeInfo) typeInfo;
        typeDesc = new TypeDesc(TypeDesc.Type.CHAR, charTypeInfo.getLength());
        break;
      case VARCHAR:
        VarcharTypeInfo varcharTypeInfo = (VarcharTypeInfo) typeInfo;
        typeDesc = new TypeDesc(TypeDesc.Type.VARCHAR, varcharTypeInfo.getLength());
        break;
      case DATE:
        typeDesc = new TypeDesc(TypeDesc.Type.DATE);
        break;
      case TIMESTAMP:
        typeDesc = new TypeDesc(TypeDesc.Type.TIMESTAMP);
        break;
      case BINARY:
        typeDesc = new TypeDesc(TypeDesc.Type.BINARY);
        break;
      case DECIMAL:
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
        typeDesc = new TypeDesc(TypeDesc.Type.DECIMAL, decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
        break;
      default:
        throw new HiveException("Unsupported type " + typeString);
    }

    return typeDesc;
  }

  private Schema convertSchema(Object obj) throws HiveException {
    org.apache.hadoop.hive.metastore.api.Schema schema = (org.apache.hadoop.hive.metastore.api.Schema) obj;
    List<FieldDesc> colDescs = new ArrayList<FieldDesc>();
    for (FieldSchema fs : schema.getFieldSchemas()) {
      String colName = fs.getName();
      String typeString = fs.getType();
      TypeDesc typeDesc = convertTypeString(typeString);
      colDescs.add(new FieldDesc(colName, typeDesc));
    }
    Schema Schema = new Schema(colDescs);
    return Schema;
  }

  @Override
  public void close() throws HiveException {
  }
}
