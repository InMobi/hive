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

package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.plan.ExplainWork;

/**
 * ExplainSemanticAnalyzer.
 *
 */
public class ExplainSemanticAnalyzer extends BaseSemanticAnalyzer {
  List<FieldSchema> fieldList;

  public ExplainSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {

    boolean extended = false;
    boolean formatted = false;
    boolean dependency = false;
    boolean logical = false;
    if (ast.getChildCount() == 2) {
      int explainOptions = ast.getChild(1).getType();
      formatted = (explainOptions == HiveParser.KW_FORMATTED);
      extended = (explainOptions == HiveParser.KW_EXTENDED);
      dependency = (explainOptions == HiveParser.KW_DEPENDENCY);
      logical = (explainOptions == HiveParser.KW_LOGICAL);
    }

    ctx.setExplain(true);
    ctx.setExplainLogical(logical);

    // Create a semantic analyzer for the query
    BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, (ASTNode) ast
        .getChild(0));
    sem.analyze((ASTNode) ast.getChild(0), ctx);
    sem.validate();

    ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
    List<Task<? extends Serializable>> tasks = sem.getRootTasks();
    Task<? extends Serializable> fetchTask = sem.getFetchTask();
    if (tasks == null) {
      if (fetchTask != null) {
        tasks = new ArrayList<Task<? extends Serializable>>();
        tasks.add(fetchTask);
      }
    } else if (fetchTask != null) {
      tasks.add(fetchTask);
    }

    ParseContext pCtx = null;
    if (sem instanceof SemanticAnalyzer) {
      pCtx = ((SemanticAnalyzer)sem).getParseContext();
    }

    Task<? extends Serializable> explTask =
        TaskFactory.get(new ExplainWork(ctx.getResFile().toString(),
        pCtx,
        tasks,
        ((ASTNode) ast.getChild(0)).toStringTree(),
        sem.getInputs(),
        extended,
        formatted,
        dependency,
        logical),
      conf);

    fieldList = explTask.getResultSchema();
    rootTasks.add(explTask);
  }

  @Override
  public List<FieldSchema> getResultSchema() {
    return fieldList;
  }
}
