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

package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Task.TaskState;
import org.apache.hadoop.hive.ql.plan.api.StageType;

public class TaskStatus {
  private String taskId;
  private StageType type;
  private String externalHandle;
  private Task.TaskState taskState;

  public TaskStatus(String taskId, String externalHandle, Task.TaskState state,
      StageType type) {
    this.taskId = taskId;
    this.type = type;
    this.externalHandle = externalHandle;
    this.taskState = state;
  }

  public TaskStatus() {

  }

  public String getTaskId() {
    return taskId;
  }

  public StageType getType() {
    return type;
  }

  public String getExternalHandle() {
    return externalHandle;
  }

  public String getTaskState() {
    return taskState.toString();
  }

  @Override
  public String toString() {
    return taskId + "/" + type + "/" + externalHandle + "/" + taskState;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public void setExternalHandle(String externalHandle) {
    this.externalHandle = externalHandle;
  }

  public void setTaskState(String taskState) {
    if (taskState == null) {
      this.taskState = TaskState.UNKNOWN_STATE;
    } else {
      this.taskState = TaskState.valueOf(taskState.toUpperCase());
    }
  }

  public void setType(StageType type) {
    this.type = type;
  }
}
