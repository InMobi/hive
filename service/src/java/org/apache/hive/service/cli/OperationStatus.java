package org.apache.hive.service.cli;

public class OperationStatus {
  private final OperationState state;
  private final String taskStatus;
  
  public OperationStatus(OperationState state, String taskStatus) {
    this.state = state;
    this.taskStatus = taskStatus;
  }
  
  public OperationState getState() {
    return state;
  }
  
  public String getTaskStatus() {
    return taskStatus;
  }
}
