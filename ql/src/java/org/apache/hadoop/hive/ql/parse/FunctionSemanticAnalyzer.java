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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CreateFunctionDesc;
import org.apache.hadoop.hive.ql.plan.DropFunctionDesc;
import org.apache.hadoop.hive.ql.plan.FunctionWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * FunctionSemanticAnalyzer.
 *
 */
public class FunctionSemanticAnalyzer extends BaseSemanticAnalyzer {
  private static final Log LOG = LogFactory
      .getLog(FunctionSemanticAnalyzer.class);

  public FunctionSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    if (ast.getToken().getType() == HiveParser.TOK_CREATEFUNCTION) {
      analyzeCreateFunction(ast);
    }
    if (ast.getToken().getType() == HiveParser.TOK_DROPFUNCTION) {
      analyzeDropFunction(ast);
    }

    LOG.info("analyze done");
  }

  private void analyzeCreateFunction(ASTNode ast) throws SemanticException {
    // ^(TOK_CREATEFUNCTION identifier StringLiteral ({isTempFunction}? => TOK_TEMPORARY))
    String functionName = ast.getChild(0).getText().toLowerCase();
    boolean isTemporaryFunction = (ast.getFirstChildWithType(HiveParser.TOK_TEMPORARY) != null);
    String className = unescapeSQLString(ast.getChild(1).getText());

    // Temp functions are not allowed to have qualified names.
    if (isTemporaryFunction && FunctionUtils.isQualifiedFunctionName(functionName)) {
      throw new SemanticException("Temporary function cannot be created with a qualified name.");
    }

    // find any referenced resources
    List<ResourceUri> resources = getResourceList(ast);
    
    CreateFunctionDesc desc =
        new CreateFunctionDesc(functionName, isTemporaryFunction, className, resources);
    rootTasks.add(TaskFactory.get(new FunctionWork(desc), conf));

    addEntities(functionName, isTemporaryFunction);
  }

  private void analyzeDropFunction(ASTNode ast) throws SemanticException {
    // ^(TOK_DROPFUNCTION identifier ifExists? $temp?)
    String functionName = ast.getChild(0).getText();
    boolean ifExists = (ast.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null);
    // we want to signal an error if the function doesn't exist and we're
    // configured not to ignore this
    boolean throwException =
      !ifExists && !HiveConf.getBoolVar(conf, ConfVars.DROPIGNORESNONEXISTENT);

    if (FunctionRegistry.getFunctionInfo(functionName) == null) {
      if (throwException) {
        throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(functionName));
      } else {
        // Fail silently
        return;
      }
    }

    boolean isTemporaryFunction = (ast.getFirstChildWithType(HiveParser.TOK_TEMPORARY) != null);
    DropFunctionDesc desc = new DropFunctionDesc(functionName, isTemporaryFunction);
    rootTasks.add(TaskFactory.get(new FunctionWork(desc), conf));

    addEntities(functionName, isTemporaryFunction);
  }

  private ResourceType getResourceType(ASTNode token) throws SemanticException {
    switch (token.getType()) {
      case HiveParser.TOK_JAR:
        return ResourceType.JAR;
      case HiveParser.TOK_FILE:
        return ResourceType.FILE;
      case HiveParser.TOK_ARCHIVE:
        return ResourceType.ARCHIVE;
      default:
        throw new SemanticException("Unexpected token " + token.toString());
    }
  }

  private List<ResourceUri> getResourceList(ASTNode ast) throws SemanticException {
    List<ResourceUri> resources = null;
    ASTNode resourcesNode = (ASTNode) ast.getFirstChildWithType(HiveParser.TOK_RESOURCE_LIST);

    if (resourcesNode != null) {
      resources = new ArrayList<ResourceUri>();
      for (int idx = 0; idx < resourcesNode.getChildCount(); ++idx) {
        // ^(TOK_RESOURCE_URI $resType $resPath)
        ASTNode resNode = (ASTNode) resourcesNode.getChild(idx);
        if (resNode.getToken().getType() != HiveParser.TOK_RESOURCE_URI) {
          throw new SemanticException("Expected token type TOK_RESOURCE_URI but found "
              + resNode.getToken().toString());
        }
        if (resNode.getChildCount() != 2) {
          throw new SemanticException("Expected 2 child nodes of TOK_RESOURCE_URI but found "
              + resNode.getChildCount());
        }
        ASTNode resTypeNode = (ASTNode) resNode.getChild(0);
        ASTNode resUriNode = (ASTNode) resNode.getChild(1);
        ResourceType resourceType = getResourceType(resTypeNode);
        resources.add(new ResourceUri(resourceType, PlanUtils.stripQuotes(resUriNode.getText())));
      }
    }

    return resources;
  }

  /**
   * Add write entities to the semantic analyzer to restrict function creation to priviliged users.
   */
  private void addEntities(String functionName, boolean isTemporaryFunction)
      throws SemanticException {
    Database database = null;
    if (isTemporaryFunction) {
      // This means temp function creation is also restricted.
      database = getDatabase(MetaStoreUtils.DEFAULT_DATABASE_NAME);
    } else {
      try {
        String[] qualifiedNameParts = FunctionUtils.getQualifiedFunctionNameParts(functionName);
        String dbName = qualifiedNameParts[0];
        database = getDatabase(dbName);
      } catch (HiveException e) {
        LOG.error(e);
        throw new SemanticException(e);
      }
    }
    if (database != null) {
      outputs.add(new WriteEntity(database, WriteEntity.WriteType.DDL_NO_LOCK));
    }
  }
}
