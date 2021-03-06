/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.llap.tezplugins;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.registry.impl.LlapFixedRegistryImpl;
import org.apache.hadoop.hive.llap.testhelpers.ControlledClock;
import org.apache.hadoop.hive.llap.tezplugins.helpers.MonotonicClock;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLlapTaskSchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(TestLlapTaskSchedulerService.class);

  private static final String HOST1 = "host1";
  private static final String HOST2 = "host2";
  private static final String HOST3 = "host3";

  @Test(timeout = 10000)
  public void testSimpleLocalAllocation() throws IOException, InterruptedException {

    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();

    try {
      Priority priority1 = Priority.newInstance(1);
      String[] hosts1 = new String[]{HOST1};

      Object task1 = new Object();
      Object clientCookie1 = new Object();

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hosts1, priority1, clientCookie1);

      tsWrapper.awaitLocalTaskAllocations(1);

      verify(tsWrapper.mockAppCallback).taskAllocated(eq(task1), eq(clientCookie1), any(Container.class));
      assertEquals(1, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(1, tsWrapper.ts.dagStats.numAllocationsPerHost.get(HOST1).get());
    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testSimpleNoLocalityAllocation() throws IOException, InterruptedException {
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();

    try {
      Priority priority1 = Priority.newInstance(1);

      Object task1 = new Object();
      Object clientCookie1 = new Object();
      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, null, priority1, clientCookie1);
      tsWrapper.awaitTotalTaskAllocations(1);
      verify(tsWrapper.mockAppCallback).taskAllocated(eq(task1), eq(clientCookie1), any(Container.class));
      assertEquals(1, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
    } finally {
      tsWrapper.shutdown();
    }
  }


  @Test(timeout = 10000)
  public void testPreemption() throws InterruptedException, IOException {

    Priority priority1 = Priority.newInstance(1);
    Priority priority2 = Priority.newInstance(2);
    String [] hosts = new String[] {HOST1};
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1);
    try {

      Object task1 = "task1";
      Object clientCookie1 = "cookie1";
      Object task2 = "task2";
      Object clientCookie2 = "cookie2";
      Object task3 = "task3";
      Object clientCookie3 = "cookie3";
      Object task4 = "task4";
      Object clientCookie4 = "cookie4";

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hosts, priority2, clientCookie1);
      tsWrapper.allocateTask(task2, hosts, priority2, clientCookie2);
      tsWrapper.allocateTask(task3, hosts, priority2, clientCookie3);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numLocalAllocations == 2) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback, times(2)).taskAllocated(any(Object.class),
          any(Object.class), any(Container.class));
      assertEquals(2, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);

      reset(tsWrapper.mockAppCallback);

      tsWrapper.allocateTask(task4, hosts, priority1, clientCookie4);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numPreemptedTasks == 1) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback).preemptContainer(any(ContainerId.class));


      tsWrapper.deallocateTask(task2, false, TaskAttemptEndReason.INTERNAL_PREEMPTION);

      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 3) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback, times(1)).taskAllocated(eq(task4),
          eq(clientCookie4), any(Container.class));

    } finally {
      tsWrapper.shutdown();
    }

  }

  @Test(timeout = 10000)
  public void testNodeDisabled() throws IOException, InterruptedException {
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(10000l);
    try {
      Priority priority1 = Priority.newInstance(1);
      String[] hosts1 = new String[]{HOST1};
      Object task1 = new Object();
      Object clientCookie1 = new Object();
      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hosts1, priority1, clientCookie1);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 1) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback).taskAllocated(eq(task1), eq(clientCookie1),
          any(Container.class));
      assertEquals(1, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
      assertEquals(0, tsWrapper.ts.dagStats.numNonLocalAllocations);
      assertEquals(1, tsWrapper.ts.dagStats.numTotalAllocations);

      tsWrapper.resetAppCallback();

      tsWrapper.clock.setTime(10000l);
      tsWrapper.rejectExecution(task1);

      // Verify that the node is blacklisted
      assertEquals(1, tsWrapper.ts.dagStats.numRejectedTasks);
      assertEquals(3, tsWrapper.ts.instanceToNodeMap.size());
      LlapTaskSchedulerService.NodeInfo disabledNodeInfo = tsWrapper.ts.disabledNodesQueue.peek();
      assertNotNull(disabledNodeInfo);
      assertEquals(HOST1, disabledNodeInfo.serviceInstance.getHost());
      assertEquals((10000l), disabledNodeInfo.getDelay(TimeUnit.MILLISECONDS));
      assertEquals((10000l + 10000l), disabledNodeInfo.expireTimeMillis);

      Object task2 = new Object();
      Object clientCookie2 = new Object();
      tsWrapper.allocateTask(task2, hosts1, priority1, clientCookie2);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 2) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback).taskAllocated(eq(task2), eq(clientCookie2), any(Container.class));
      assertEquals(1, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
      assertEquals(1, tsWrapper.ts.dagStats.numNonLocalAllocations);
      assertEquals(2, tsWrapper.ts.dagStats.numTotalAllocations);

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testNodeReEnabled() throws InterruptedException, IOException {
    // Based on actual timing.
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(1000l);
    try {
      Priority priority1 = Priority.newInstance(1);
      String[] hosts1 = new String[]{HOST1};
      String[] hosts2 = new String[]{HOST2};
      String[] hosts3 = new String[]{HOST3};

      Object task1 = new Object();
      Object clientCookie1 = new Object();
      Object task2 = new Object();
      Object clientCookie2 = new Object();
      Object task3 = new Object();
      Object clientCookie3 = new Object();

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hosts1, priority1, clientCookie1);
      tsWrapper.allocateTask(task2, hosts2, priority1, clientCookie2);
      tsWrapper.allocateTask(task3, hosts3, priority1, clientCookie3);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 3) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback, times(3)).taskAllocated(any(Object.class), any(Object.class), any(Container.class));
      assertEquals(3, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
      assertEquals(3, tsWrapper.ts.dagStats.numTotalAllocations);

      tsWrapper.resetAppCallback();

      tsWrapper.rejectExecution(task1);
      tsWrapper.rejectExecution(task2);
      tsWrapper.rejectExecution(task3);

      // Verify that the node is blacklisted
      assertEquals(3, tsWrapper.ts.dagStats.numRejectedTasks);
      assertEquals(3, tsWrapper.ts.instanceToNodeMap.size());
      assertEquals(3, tsWrapper.ts.disabledNodesQueue.size());


      Object task4 = new Object();
      Object clientCookie4 = new Object();
      Object task5 = new Object();
      Object clientCookie5 = new Object();
      Object task6 = new Object();
      Object clientCookie6 = new Object();
      tsWrapper.allocateTask(task4, hosts1, priority1, clientCookie4);
      tsWrapper.allocateTask(task5, hosts2, priority1, clientCookie5);
      tsWrapper.allocateTask(task6, hosts3, priority1, clientCookie6);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 6) {
          break;
        }
      }

      ArgumentCaptor<Container> argumentCaptor = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(3)).taskAllocated(any(Object.class), any(Object.class), argumentCaptor.capture());

      // which affects the locality matching
      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
      assertEquals(6, tsWrapper.ts.dagStats.numTotalAllocations);

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testForceLocalityTest1() throws IOException, InterruptedException {
    // 2 hosts. 2 per host. 5 requests at the same priority.
    // First 3 on host1, Next at host2, Last with no host.
    // Third request on host1 should not be allocated immediately.
    forceLocalityTest1(true);

  }

  @Test(timeout = 10000)
  public void testNoForceLocalityCounterTest1() throws IOException, InterruptedException {
    // 2 hosts. 2 per host. 5 requests at the same priority.
    // First 3 on host1, Next at host2, Last with no host.
    // Third should allocate on host2, 4th on host2, 5th will wait.

    forceLocalityTest1(false);
  }

  private void forceLocalityTest1(boolean forceLocality) throws IOException, InterruptedException {
    Priority priority1 = Priority.newInstance(1);

    String[] hosts = new String[] {HOST1, HOST2};

    String[] hostsH1 = new String[] {HOST1};
    String[] hostsH2 = new String[] {HOST2};

    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, (forceLocality ? -1l : 0l));

    try {
      Object task1 = "task1";
      Object clientCookie1 = "cookie1";
      Object task2 = "task2";
      Object clientCookie2 = "cookie2";
      Object task3 = "task3";
      Object clientCookie3 = "cookie3";
      Object task4 = "task4";
      Object clientCookie4 = "cookie4";
      Object task5 = "task5";
      Object clientCookie5 = "cookie5";

      tsWrapper.controlScheduler(true);
      //H1 - should allocate
      tsWrapper.allocateTask(task1, hostsH1, priority1, clientCookie1);
      //H1 - should allocate
      tsWrapper.allocateTask(task2, hostsH1, priority1, clientCookie2);
      //H1 - no capacity if force, should allocate otherwise
      tsWrapper.allocateTask(task3, hostsH1, priority1, clientCookie3);
      //H2 - should allocate
      tsWrapper.allocateTask(task4, hostsH2, priority1, clientCookie4);
      //No location - should allocate if force, no capacity otherwise
      tsWrapper.allocateTask(task5, null, priority1, clientCookie5);

      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 4) {
          break;
        }
      }

      // Verify no preemption requests - since everything is at the same priority
      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(4)).taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(4, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));
      if (forceLocality) {
        // task3 not allocated
        assertEquals(task4, argumentCaptor.getAllValues().get(2));
        assertEquals(task5, argumentCaptor.getAllValues().get(3));
      } else {
        assertEquals(task3, argumentCaptor.getAllValues().get(2));
        assertEquals(task4, argumentCaptor.getAllValues().get(3));
      }

      //Complete one task on host1.
      tsWrapper.deallocateTask(task1, true, null);

      reset(tsWrapper.mockAppCallback);

      // Try scheduling again.
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 5) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(1)).taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(1, argumentCaptor.getAllValues().size());
      if (forceLocality) {
        assertEquals(task3, argumentCaptor.getAllValues().get(0));
      } else {
        assertEquals(task5, argumentCaptor.getAllValues().get(0));
      }

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testForcedLocalityUnknownHost() throws IOException, InterruptedException {
    Priority priority1 = Priority.newInstance(1);

    String[] hostsKnown = new String[]{HOST1};
    String[] hostsUnknown = new String[]{HOST2};

    TestTaskSchedulerServiceWrapper tsWrapper =
        new TestTaskSchedulerServiceWrapper(2000, hostsKnown, 1, 1, -1l);
    try {
      Object task1 = "task1";
      Object clientCookie1 = "cookie1";

      Object task2 = "task2";
      Object clientCookie2 = "cookie2";

      tsWrapper.controlScheduler(true);
      // Should allocate since H2 is not known.
      tsWrapper.allocateTask(task1, hostsUnknown, priority1, clientCookie1);
      tsWrapper.allocateTask(task2, hostsKnown, priority1, clientCookie2);


      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 2) {
          break;
        }
      }

      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));


    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testForcedLocalityPreemption() throws IOException, InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    Priority priority2 = Priority.newInstance(2);
    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(2000, hosts, 2, 0, -1l);

    // Fill up host1 with p2 tasks.
    // Leave host2 empty
    // Try running p1 task on host1 - should preempt

    try {
      Object task1 = "task1";
      Object clientCookie1 = "cookie1";
      Object task2 = "task2";
      Object clientCookie2 = "cookie2";
      Object task3 = "task3";
      Object clientCookie3 = "cookie3";
      Object task4 = "task4";
      Object clientCookie4 = "cookie4";

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hostsH1, priority2, clientCookie1);
      tsWrapper.allocateTask(task2, hostsH1, priority2, clientCookie2);
      // This request at a lower priority should not affect anything.
      tsWrapper.allocateTask(task3, hostsH1, priority2, clientCookie3);
      tsWrapper.awaitLocalTaskAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));

      reset(tsWrapper.mockAppCallback);
      // Allocate t4 at higher priority. t3 should not be allocated,
      // and a preemption should be attempted on host1, despite host2 having available capacity
      tsWrapper.allocateTask(task4, hostsH1, priority1, clientCookie4);

      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numPreemptedTasks == 1) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback).preemptContainer(any(ContainerId.class));

      tsWrapper.deallocateTask(task1, false, TaskAttemptEndReason.INTERNAL_PREEMPTION);

      tsWrapper.awaitLocalTaskAllocations(3);

      verify(tsWrapper.mockAppCallback, times(1)).taskAllocated(eq(task4),
          eq(clientCookie4), any(Container.class));

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testPreemptionChoiceTimeOrdering() throws IOException, InterruptedException {

    Priority priority1 = Priority.newInstance(1);
    Priority priority2 = Priority.newInstance(2);

    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, -1l);

    try {
      Object task1 = "task1";
      Object clientCookie1 = "cookie1";
      Object task2 = "task2";
      Object clientCookie2 = "cookie2";
      Object task3 = "task3";
      Object clientCookie3 = "cookie3";

      tsWrapper.controlScheduler(true);

      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> cArgCaptor = ArgumentCaptor.forClass(Container.class);

      // Request task1
      tsWrapper.getClock().setTime(10000l);
      tsWrapper.allocateTask(task1, hostsH1, priority2, clientCookie1);
      tsWrapper.awaitLocalTaskAllocations(1);
      verify(tsWrapper.mockAppCallback, times(1))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), cArgCaptor.capture());
      ContainerId t1Cid = cArgCaptor.getValue().getId();

      reset(tsWrapper.mockAppCallback);
      // Move clock backwards (so that t1 allocation is after t2 allocation)
      // Request task2 (task1 already started at previously set time)
      tsWrapper.getClock().setTime(tsWrapper.getClock().getTime() - 1000);
      tsWrapper.allocateTask(task2, hostsH1, priority2, clientCookie2);
      tsWrapper.awaitLocalTaskAllocations(2);
      verify(tsWrapper.mockAppCallback, times(1))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), cArgCaptor.capture());


      reset(tsWrapper.mockAppCallback);
      // Move clock forward, and request a task at p=1
      tsWrapper.getClock().setTime(tsWrapper.getClock().getTime() + 2000);

      tsWrapper.allocateTask(task3, hostsH1, priority1, clientCookie3);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numPreemptedTasks == 1) {
          break;
        }
      }
      // Ensure task1 is preempted based on time (match it's allocated containerId)
      ArgumentCaptor<ContainerId> cIdArgCaptor = ArgumentCaptor.forClass(ContainerId.class);
      verify(tsWrapper.mockAppCallback).preemptContainer(cIdArgCaptor.capture());
      assertEquals(t1Cid, cIdArgCaptor.getValue());

    } finally {
      tsWrapper.shutdown();
    }

  }

  @Test(timeout = 10000)
  public void testForcedLocalityMultiplePreemptionsSameHost1() throws IOException,
      InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    Priority priority2 = Priority.newInstance(2);

    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, -1l);

    // Fill up host1 with p2 tasks.
    // Leave host2 empty
    // Try running p1 task on host1 - should preempt
    // Await preemption request.
    // Try running another p1 task on host1 - should preempt
    // Await preemption request.

    try {

      Object task1 = "task1";
      Object clientCookie1 = "cookie1";
      Object task2 = "task2";
      Object clientCookie2 = "cookie2";
      Object task3 = "task3";
      Object clientCookie3 = "cookie3";
      Object task4 = "task4";
      Object clientCookie4 = "cookie4";

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hostsH1, priority2, clientCookie1);
      tsWrapper.allocateTask(task2, hostsH1, priority2, clientCookie2);

      tsWrapper.awaitLocalTaskAllocations(2);
      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> cArgCaptor = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), cArgCaptor.capture());
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));
      assertEquals(2, cArgCaptor.getAllValues().size());
      ContainerId t1CId = cArgCaptor.getAllValues().get(0).getId();

      reset(tsWrapper.mockAppCallback);
      // At this point. 2 tasks running - both at priority 2.
      // Try running a priority 1 task
      tsWrapper.allocateTask(task3, hostsH1, priority1, clientCookie3);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numPreemptedTasks == 1) {
          break;
        }
      }
      ArgumentCaptor<ContainerId> cIdArgCaptor = ArgumentCaptor.forClass(ContainerId.class);
      verify(tsWrapper.mockAppCallback).preemptContainer(cIdArgCaptor.capture());

      // Determin which task has been preempted. Normally task2 would be preempted based on it starting
      // later. However - both may have the same start time, so either could be picked.
      Object deallocatedTask1; // De-allocated now
      Object deallocatedTask2; // Will be de-allocated later.
      if (cIdArgCaptor.getValue().equals(t1CId)) {
        deallocatedTask1 = task1;
        deallocatedTask2 = task2;
      } else {
        deallocatedTask1 = task2;
        deallocatedTask2 = task1;
      }

      tsWrapper.deallocateTask(deallocatedTask1, false, TaskAttemptEndReason.INTERNAL_PREEMPTION);

      tsWrapper.awaitLocalTaskAllocations(3);

      verify(tsWrapper.mockAppCallback, times(1)).taskAllocated(eq(task3),
          eq(clientCookie3), any(Container.class));

      reset(tsWrapper.mockAppCallback);
      // At this point. one p=2 task and task3(p=1) running. Ask for another p1 task.
      tsWrapper.allocateTask(task4, hostsH1, priority1, clientCookie4);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numPreemptedTasks == 2) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback).preemptContainer(any(ContainerId.class));

      tsWrapper.deallocateTask(deallocatedTask2, false, TaskAttemptEndReason.INTERNAL_PREEMPTION);

      tsWrapper.awaitLocalTaskAllocations(4);

      verify(tsWrapper.mockAppCallback, times(1)).taskAllocated(eq(task4),
          eq(clientCookie4), any(Container.class));


    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testForcedLocalityMultiplePreemptionsSameHost2() throws IOException,
      InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    Priority priority2 = Priority.newInstance(2);

    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, -1l);

    // Fill up host1 with p2 tasks.
    // Leave host2 empty
    // Try running both p1 tasks on host1.
    // R: Single preemption triggered, followed by allocation, followed by another preemption.
    //

    try {

      Object task1 = "task1";
      Object clientCookie1 = "cookie1";
      Object task2 = "task2";
      Object clientCookie2 = "cookie2";
      Object task3 = "task3";
      Object clientCookie3 = "cookie3";
      Object task4 = "task4";
      Object clientCookie4 = "cookie4";

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hostsH1, priority2, clientCookie1);
      tsWrapper.allocateTask(task2, hostsH1, priority2, clientCookie2);

      tsWrapper.awaitLocalTaskAllocations(2);
      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> cArgCaptor = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), cArgCaptor.capture());
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));
      assertEquals(2, cArgCaptor.getAllValues().size());
      ContainerId t1CId = cArgCaptor.getAllValues().get(0).getId();

      reset(tsWrapper.mockAppCallback);
      // At this point. 2 tasks running - both at priority 2.
      // Try running a priority 1 task
      tsWrapper.allocateTask(task3, hostsH1, priority1, clientCookie3);
      tsWrapper.allocateTask(task4, hostsH1, priority1, clientCookie4);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numPreemptedTasks == 1) {
          break;
        }
      }
      ArgumentCaptor<ContainerId> cIdArgCaptor = ArgumentCaptor.forClass(ContainerId.class);
      verify(tsWrapper.mockAppCallback).preemptContainer(cIdArgCaptor.capture());

      // Determin which task has been preempted. Normally task2 would be preempted based on it starting
      // later. However - both may have the same start time, so either could be picked.
      Object deallocatedTask1; // De-allocated now
      Object deallocatedTask2; // Will be de-allocated later.
      if (cIdArgCaptor.getValue().equals(t1CId)) {
        deallocatedTask1 = task1;
        deallocatedTask2 = task2;
      } else {
        deallocatedTask1 = task2;
        deallocatedTask2 = task1;
      }

      tsWrapper.deallocateTask(deallocatedTask1, false, TaskAttemptEndReason.INTERNAL_PREEMPTION);

      tsWrapper.awaitLocalTaskAllocations(3);

      verify(tsWrapper.mockAppCallback, times(1)).taskAllocated(eq(task3),
          eq(clientCookie3), any(Container.class));

      // At this point. one p=2 task and task3(p=1) running. Ask for another p1 task.
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numPreemptedTasks == 2) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback, times(2)).preemptContainer(any(ContainerId.class));

      tsWrapper.deallocateTask(deallocatedTask2, false, TaskAttemptEndReason.INTERNAL_PREEMPTION);

      tsWrapper.awaitLocalTaskAllocations(4);

      verify(tsWrapper.mockAppCallback, times(1)).taskAllocated(eq(task4),
          eq(clientCookie4), any(Container.class));


    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testForcedLocalityNotInDelayedQueue() throws IOException, InterruptedException {
    String[] hosts = new String[]{HOST1, HOST2};

    String[] hostsH1 = new String[]{HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper =
        new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, -1l);
    testNotInQueue(tsWrapper, hostsH1);
  }

  @Test(timeout = 10000)
  public void testNoLocalityNotInDelayedQueue() throws IOException, InterruptedException {
    String[] hosts = new String[]{HOST1};

    String[] hostsH1 = new String[]{HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper =
        new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, 0l);
    testNotInQueue(tsWrapper, hostsH1);
  }

  private void testNotInQueue(TestTaskSchedulerServiceWrapper tsWrapper, String[] hosts) throws
      InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    try {
      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(hosts, priority1);
      tsWrapper.allocateTask(hosts, priority1);
      tsWrapper.allocateTask(hosts, priority1); // 1 more than capacity.

      tsWrapper.awaitLocalTaskAllocations(2);

      assertEquals(0, tsWrapper.ts.delayedTaskQueue.size());

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testDelayedLocalityFallbackToNonLocal() throws IOException, InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper =
        new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, 10000l, true);
    LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled
        delayedTaskSchedulerCallableControlled =
        (LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled) tsWrapper.ts.delayedTaskSchedulerCallable;
    ControlledClock clock = tsWrapper.getClock();
    clock.setTime(clock.getTime());

    // Fill up host1 with tasks. Leave host2 empty.
    try {
      tsWrapper.controlScheduler(true);
      Object task1 = tsWrapper.allocateTask(hostsH1, priority1);
      Object task2 = tsWrapper.allocateTask(hostsH1, priority1);
      Object task3 = tsWrapper.allocateTask(hostsH1, priority1); // 1 more than capacity.

      tsWrapper.awaitLocalTaskAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));

      reset(tsWrapper.mockAppCallback);

      // No capacity left on node1. The next task should be allocated to node2 after it times out.
      clock.setTime(clock.getTime() + 10000l); // Past the timeout.

      assertEquals(
          LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled.STATE_NOT_RUN,
          delayedTaskSchedulerCallableControlled.lastState);

      delayedTaskSchedulerCallableControlled.triggerGetNextTask();
      delayedTaskSchedulerCallableControlled.awaitGetNextTaskProcessing();

      // Verify that an attempt was made to schedule the task, but the decision was to skip scheduling
      assertEquals(
          LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled.STATE_RETURNED_TASK,
          delayedTaskSchedulerCallableControlled.lastState);
      assertTrue(delayedTaskSchedulerCallableControlled.shouldScheduleTaskTriggered &&
          delayedTaskSchedulerCallableControlled.lastShouldScheduleTaskResult);

      tsWrapper.awaitChangeInTotalAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> containerCaptor = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(1))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), containerCaptor.capture());
      assertEquals(1, argumentCaptor.getAllValues().size());
      assertEquals(task3, argumentCaptor.getAllValues().get(0));
      Container assignedContainer = containerCaptor.getValue();
      assertEquals(HOST2, assignedContainer.getNodeId().getHost());


      assertEquals(2, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(1, tsWrapper.ts.dagStats.numNonLocalAllocations);
      assertEquals(1, tsWrapper.ts.dagStats.numDelayedAllocations);
      assertEquals(2, tsWrapper.ts.dagStats.numAllocationsPerHost.get(HOST1).get());
      assertEquals(1, tsWrapper.ts.dagStats.numAllocationsPerHost.get(HOST2).get());

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testDelayedLocalityDelayedAllocation() throws InterruptedException, IOException {
    Priority priority1 = Priority.newInstance(1);
    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper =
        new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, 10000l, true);
    LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled
        delayedTaskSchedulerCallableControlled =
        (LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled) tsWrapper.ts.delayedTaskSchedulerCallable;
    ControlledClock clock = tsWrapper.getClock();
    clock.setTime(clock.getTime());

    // Fill up host1 with tasks. Leave host2 empty.
    try {
      tsWrapper.controlScheduler(true);
      Object task1 = tsWrapper.allocateTask(hostsH1, priority1);
      Object task2 = tsWrapper.allocateTask(hostsH1, priority1);
      Object task3 = tsWrapper.allocateTask(hostsH1, priority1); // 1 more than capacity.

      tsWrapper.awaitLocalTaskAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));

      reset(tsWrapper.mockAppCallback);

      // Move the clock forward 2000ms, and check the delayed queue
      clock.setTime(clock.getTime() + 2000l); // Past the timeout.

      assertEquals(
          LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled.STATE_NOT_RUN,
          delayedTaskSchedulerCallableControlled.lastState);

      delayedTaskSchedulerCallableControlled.triggerGetNextTask();
      delayedTaskSchedulerCallableControlled.awaitGetNextTaskProcessing();

      // Verify that an attempt was made to schedule the task, but the decision was to skip scheduling
      assertEquals(
          LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled.STATE_TIMEOUT_NOT_EXPIRED,
          delayedTaskSchedulerCallableControlled.lastState);
      assertFalse(delayedTaskSchedulerCallableControlled.shouldScheduleTaskTriggered);

      tsWrapper.deallocateTask(task1, true, null);

      // Node1 now has free capacity. task1 should be allocated to it.
      tsWrapper.awaitChangeInTotalAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> containerCaptor = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(1))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), containerCaptor.capture());
      assertEquals(1, argumentCaptor.getAllValues().size());
      assertEquals(task3, argumentCaptor.getAllValues().get(0));
      Container assignedContainer = containerCaptor.getValue();
      assertEquals(HOST1, assignedContainer.getNodeId().getHost());


      assertEquals(3, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(0, tsWrapper.ts.dagStats.numNonLocalAllocations);
      assertEquals(1, tsWrapper.ts.dagStats.numDelayedAllocations);
      assertEquals(3, tsWrapper.ts.dagStats.numAllocationsPerHost.get(HOST1).get());

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testDelayedQueeTaskSelectionAfterScheduled() throws IOException,
      InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper =
        new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, 10000l, true);
    LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled
        delayedTaskSchedulerCallableControlled =
        (LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled) tsWrapper.ts.delayedTaskSchedulerCallable;
    ControlledClock clock = tsWrapper.getClock();
    clock.setTime(clock.getTime());

    // Fill up host1 with tasks. Leave host2 empty.
    try {
      tsWrapper.controlScheduler(true);
      Object task1 = tsWrapper.allocateTask(hostsH1, priority1);
      Object task2 = tsWrapper.allocateTask(hostsH1, priority1);
      Object task3 = tsWrapper.allocateTask(hostsH1, priority1); // 1 more than capacity.

      tsWrapper.awaitLocalTaskAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));

      // Simulate a 2s delay before finishing the task.
      clock.setTime(clock.getTime() + 2000);

      assertEquals(
          LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled.STATE_NOT_RUN,
          delayedTaskSchedulerCallableControlled.lastState);

      delayedTaskSchedulerCallableControlled.triggerGetNextTask();
      delayedTaskSchedulerCallableControlled.awaitGetNextTaskProcessing();
      assertEquals(
          LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled.STATE_TIMEOUT_NOT_EXPIRED,
          delayedTaskSchedulerCallableControlled.lastState);
      assertFalse(delayedTaskSchedulerCallableControlled.shouldScheduleTaskTriggered);

      reset(tsWrapper.mockAppCallback);

      // Now finish task1, which will make capacity for task3 to run. Nothing is coming out of the delayed queue yet.
      tsWrapper.deallocateTask(task1, true, null);
      tsWrapper.awaitLocalTaskAllocations(3);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> containerCaptor = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(1))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), containerCaptor.capture());
      assertEquals(1, argumentCaptor.getAllValues().size());
      assertEquals(task3, argumentCaptor.getAllValues().get(0));
      Container assignedContainer = containerCaptor.getValue();
      assertEquals(HOST1, assignedContainer.getNodeId().getHost());

      reset(tsWrapper.mockAppCallback);

      // Move the clock forward and trigger a run.
      clock.setTime(clock.getTime() + 8000); // Set to start + 10000 which is the timeout
      delayedTaskSchedulerCallableControlled.triggerGetNextTask();
      delayedTaskSchedulerCallableControlled.awaitGetNextTaskProcessing();
      assertEquals(
          LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled.STATE_RETURNED_TASK,
          delayedTaskSchedulerCallableControlled.lastState);
      // Verify that an attempt was made to schedule the task, but the decision was to skip scheduling
      assertTrue(delayedTaskSchedulerCallableControlled.shouldScheduleTaskTriggered &&
          !delayedTaskSchedulerCallableControlled.lastShouldScheduleTaskResult);

      // Ensure there's no more invocations.
      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      verify(tsWrapper.mockAppCallback, never()).taskAllocated(any(Object.class), any(Object.class), any(Container.class));

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testTaskInfoDelay() {

    LlapTaskSchedulerService.LocalityDelayConf localityDelayConf1 =
        new LlapTaskSchedulerService.LocalityDelayConf(3000);

    ControlledClock clock = new ControlledClock(new MonotonicClock());
    clock.setTime(clock.getTime());


    // With a timeout of 3000.
    LlapTaskSchedulerService.TaskInfo taskInfo =
        new LlapTaskSchedulerService.TaskInfo(localityDelayConf1, clock, new Object(), new Object(),
            mock(Priority.class), mock(Resource.class), null, null, clock.getTime());

    assertFalse(taskInfo.shouldForceLocality());

    assertEquals(3000, taskInfo.getDelay(TimeUnit.MILLISECONDS));
    assertTrue(taskInfo.shouldDelayForLocality(clock.getTime()));

    clock.setTime(clock.getTime() + 500);
    assertEquals(2500, taskInfo.getDelay(TimeUnit.MILLISECONDS));
    assertTrue(taskInfo.shouldDelayForLocality(clock.getTime()));

    clock.setTime(clock.getTime() + 2500);
    assertEquals(0, taskInfo.getDelay(TimeUnit.MILLISECONDS));
    assertFalse(taskInfo.shouldDelayForLocality(clock.getTime()));


    // No locality delay
    LlapTaskSchedulerService.LocalityDelayConf localityDelayConf2 =
        new LlapTaskSchedulerService.LocalityDelayConf(0);
    taskInfo =
        new LlapTaskSchedulerService.TaskInfo(localityDelayConf2, clock, new Object(), new Object(),
            mock(Priority.class), mock(Resource.class), null, null, clock.getTime());
    assertFalse(taskInfo.shouldDelayForLocality(clock.getTime()));
    assertFalse(taskInfo.shouldForceLocality());
    assertTrue(taskInfo.getDelay(TimeUnit.MILLISECONDS) < 0);

    // Force locality
    LlapTaskSchedulerService.LocalityDelayConf localityDelayConf3 =
        new LlapTaskSchedulerService.LocalityDelayConf(-1);
    taskInfo =
        new LlapTaskSchedulerService.TaskInfo(localityDelayConf3, clock, new Object(), new Object(),
            mock(Priority.class), mock(Resource.class), null, null, clock.getTime());
    assertTrue(taskInfo.shouldDelayForLocality(clock.getTime()));
    assertTrue(taskInfo.shouldForceLocality());
    assertFalse(taskInfo.getDelay(TimeUnit.MILLISECONDS) < 0);
  }

  @Test(timeout = 10000)
  public void testLocalityDelayTaskOrdering() throws InterruptedException, IOException {

    LlapTaskSchedulerService.LocalityDelayConf localityDelayConf =
        new LlapTaskSchedulerService.LocalityDelayConf(3000);

    ControlledClock clock = new ControlledClock(new MonotonicClock());
    clock.setTime(clock.getTime());

    DelayQueue<LlapTaskSchedulerService.TaskInfo> delayedQueue = new DelayQueue<>();

    LlapTaskSchedulerService.TaskInfo taskInfo1 =
        new LlapTaskSchedulerService.TaskInfo(localityDelayConf, clock, new Object(), new Object(),
            mock(Priority.class), mock(Resource.class), null, null, clock.getTime());

    clock.setTime(clock.getTime() + 1000);
    LlapTaskSchedulerService.TaskInfo taskInfo2 =
        new LlapTaskSchedulerService.TaskInfo(localityDelayConf, clock, new Object(), new Object(),
            mock(Priority.class), mock(Resource.class), null, null, clock.getTime());

    delayedQueue.add(taskInfo1);
    delayedQueue.add(taskInfo2);

    assertEquals(taskInfo1, delayedQueue.peek());
  }

  @Test (timeout = 15000)
  public void testDelayedLocalityNodeCommErrorImmediateAllocation() throws IOException, InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    // Node disable timeout higher than locality delay.
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(20000, hosts, 1, 1, 10000l);

    // Fill up host1 with tasks. Leave host2 empty.
    try {
      long startTime = tsWrapper.getClock().getTime();
      tsWrapper.controlScheduler(true);
      Object task1 = tsWrapper.allocateTask(hostsH1, priority1);
      Object task2 = tsWrapper.allocateTask(hostsH1, priority1);
      Object task3 = tsWrapper.allocateTask(hostsH1, priority1); // 1 more than capacity.

      tsWrapper.awaitLocalTaskAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));

      reset(tsWrapper.mockAppCallback);

      // Mark a task as failed due to a comm failure.
      tsWrapper.deallocateTask(task1, false, TaskAttemptEndReason.COMMUNICATION_ERROR);

      // Node1 marked as failed, node2 has capacity.
      // Timeout for nodes is larger than delay - immediate allocation
      tsWrapper.awaitChangeInTotalAllocations(2);

      long thirdAllocateTime = tsWrapper.getClock().getTime();
      long diff = thirdAllocateTime - startTime;
      // diffAfterSleep < total sleepTime
      assertTrue("Task not allocated in expected time window: duration=" + diff, diff < 10000l);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> containerCaptor = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(1))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), containerCaptor.capture());
      assertEquals(1, argumentCaptor.getAllValues().size());
      assertEquals(task3, argumentCaptor.getAllValues().get(0));
      Container assignedContainer = containerCaptor.getValue();
      assertEquals(HOST2, assignedContainer.getNodeId().getHost());


      assertEquals(2, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(1, tsWrapper.ts.dagStats.numNonLocalAllocations);
      assertEquals(1, tsWrapper.ts.dagStats.numDelayedAllocations);
      assertEquals(2, tsWrapper.ts.dagStats.numAllocationsPerHost.get(HOST1).get());
      assertEquals(1, tsWrapper.ts.dagStats.numAllocationsPerHost.get(HOST2).get());

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test (timeout = 15000)
  public void testDelayedLocalityNodeCommErrorDelayedAllocation() throws IOException, InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper =
        new TestTaskSchedulerServiceWrapper(5000, hosts, 1, 1, 10000l, true);
    LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled
        delayedTaskSchedulerCallableControlled =
        (LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled) tsWrapper.ts.delayedTaskSchedulerCallable;
    ControlledClock clock = tsWrapper.getClock();
    clock.setTime(clock.getTime());

    // Fill up host1 with tasks. Leave host2 empty.
    try {
      tsWrapper.controlScheduler(true);
      Object task1 = tsWrapper.allocateTask(hostsH1, priority1);
      Object task2 = tsWrapper.allocateTask(hostsH1, priority1);
      Object task3 = tsWrapper.allocateTask(hostsH1, priority1); // 1 more than capacity.

      tsWrapper.awaitLocalTaskAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));

      reset(tsWrapper.mockAppCallback);

      // Mark a task as failed due to a comm failure.
      tsWrapper.deallocateTask(task1, false, TaskAttemptEndReason.COMMUNICATION_ERROR);

      // Node1 has free capacity but is disabled. Node 2 has capcaity. Delay > re-enable tiemout
      tsWrapper.ensureNoChangeInTotalAllocations(2, 2000l);
    } finally {
      tsWrapper.shutdown();
    }
  }

  private static class TestTaskSchedulerServiceWrapper {
    static final Resource resource = Resource.newInstance(1024, 1);
    Configuration conf;
    TaskSchedulerContext mockAppCallback = mock(TaskSchedulerContext.class);
    ControlledClock clock = new ControlledClock(new MonotonicClock());
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1000, 1), 1);
    LlapTaskSchedulerServiceForTest ts;

    TestTaskSchedulerServiceWrapper() throws IOException, InterruptedException {
      this(2000l);
    }

    TestTaskSchedulerServiceWrapper(long disableTimeoutMillis) throws IOException,
        InterruptedException {
      this(disableTimeoutMillis, new String[]{HOST1, HOST2, HOST3}, 4,
          ConfVars.LLAP_DAEMON_TASK_SCHEDULER_WAIT_QUEUE_SIZE.defaultIntVal);
    }

    TestTaskSchedulerServiceWrapper(long disableTimeoutMillis, String[] hosts, int numExecutors, int waitQueueSize) throws
        IOException, InterruptedException {
      this(disableTimeoutMillis, hosts, numExecutors, waitQueueSize, 0l);
    }

    TestTaskSchedulerServiceWrapper(long nodeDisableTimeoutMillis, String[] hosts, int numExecutors,
                                    int waitQueueSize, long localityDelayMs) throws
        IOException, InterruptedException {
      this(nodeDisableTimeoutMillis, hosts, numExecutors, waitQueueSize, localityDelayMs, false);
    }

    TestTaskSchedulerServiceWrapper(long nodeDisableTimeoutMillis, String[] hosts, int numExecutors,
                                    int waitQueueSize, long localityDelayMs, boolean controlledDelayedTaskQueue) throws
        IOException, InterruptedException {
      conf = new Configuration();
      conf.setStrings(ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname, hosts);
      conf.setInt(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname, numExecutors);
      conf.setInt(ConfVars.LLAP_DAEMON_TASK_SCHEDULER_WAIT_QUEUE_SIZE.varname, waitQueueSize);
      conf.set(ConfVars.LLAP_TASK_SCHEDULER_NODE_REENABLE_MIN_TIMEOUT_MS.varname,
          nodeDisableTimeoutMillis + "ms");
      conf.setBoolean(LlapFixedRegistryImpl.FIXED_REGISTRY_RESOLVE_HOST_NAMES, false);
      conf.setLong(ConfVars.LLAP_TASK_SCHEDULER_LOCALITY_DELAY.varname, localityDelayMs);

      doReturn(appAttemptId).when(mockAppCallback).getApplicationAttemptId();
      doReturn(11111l).when(mockAppCallback).getCustomClusterIdentifier();
      UserPayload userPayload = TezUtils.createUserPayloadFromConf(conf);
      doReturn(userPayload).when(mockAppCallback).getInitialUserPayload();

      if (controlledDelayedTaskQueue) {
        ts = new LlapTaskSchedulerServiceForTestControlled(mockAppCallback, clock);
      } else {
        ts = new LlapTaskSchedulerServiceForTest(mockAppCallback, clock);
      }

      controlScheduler(true);
      ts.initialize();
      ts.start();
      // One scheduler pass from the nodes that are added at startup
      signalSchedulerRun();
      controlScheduler(false);
      awaitSchedulerRun();
    }

    ControlledClock getClock() {
      return clock;
    }

    void controlScheduler(boolean val) {
      ts.forTestsetControlScheduling(val);
    }

    void signalSchedulerRun() throws InterruptedException {
      ts.forTestSignalSchedulingRun();
    }

    void awaitSchedulerRun() throws InterruptedException {
      ts.forTestAwaitSchedulingRun(-1);
    }

    /**
     *
     * @param timeoutMs
     * @return false if the time elapsed
     * @throws InterruptedException
     */
    boolean awaitSchedulerRun(long timeoutMs) throws InterruptedException {
      return ts.forTestAwaitSchedulingRun(timeoutMs);
    }

    void resetAppCallback() {
      reset(mockAppCallback);
    }

    void shutdown() {
      ts.shutdown();
    }

    void allocateTask(Object task, String[] hosts, Priority priority, Object clientCookie) {
      ts.allocateTask(task, resource, hosts, null, priority, null, clientCookie);
    }



    void deallocateTask(Object task, boolean succeeded, TaskAttemptEndReason endReason) {
      ts.deallocateTask(task, succeeded, endReason, null);
    }

    void rejectExecution(Object task) {
      ts.deallocateTask(task, false, TaskAttemptEndReason.EXECUTOR_BUSY, null);
    }


    // More complex methods which may wrap multiple operations
    Object allocateTask(String[] hosts, Priority priority) {
      Object task = new Object();
      Object clientCookie = new Object();
      allocateTask(task, hosts, priority, clientCookie);
      return task;
    }

    public void awaitTotalTaskAllocations(int numTasks) throws InterruptedException {
      while (true) {
        signalSchedulerRun();
        awaitSchedulerRun();
        if (ts.dagStats.numTotalAllocations == numTasks) {
          break;
        }
      }
    }

    public void awaitLocalTaskAllocations(int numTasks) throws InterruptedException {
      while (true) {
        signalSchedulerRun();
        awaitSchedulerRun();
        if (ts.dagStats.numLocalAllocations == numTasks) {
          break;
        }
      }
    }

    public void awaitChangeInTotalAllocations(int previousAllocations) throws InterruptedException {
      while (true) {
        signalSchedulerRun();
        awaitSchedulerRun();
        if (ts.dagStats.numTotalAllocations > previousAllocations) {
          break;
        }
        Thread.sleep(200l);
      }
    }

    public void ensureNoChangeInTotalAllocations(int previousAllocations, long timeout) throws
        InterruptedException {
      long startTime = Time.monotonicNow();
      long timeLeft = timeout;
      while (timeLeft > 0) {
        signalSchedulerRun();
        awaitSchedulerRun(Math.min(200, timeLeft));
        if (ts.dagStats.numTotalAllocations != previousAllocations) {
          throw new IllegalStateException("NumTotalAllocations expected to stay at " + previousAllocations + ". Actual=" + ts.dagStats.numTotalAllocations);
        }
        timeLeft = (startTime + timeout) - Time.monotonicNow();
      }
    }
  }

  private static class LlapTaskSchedulerServiceForTest extends LlapTaskSchedulerService {

    private AtomicBoolean controlScheduling = new AtomicBoolean(false);
    private final Lock testLock = new ReentrantLock();
    private final Condition schedulingCompleteCondition = testLock.newCondition();
    private boolean schedulingComplete = false;
    private final Condition triggerSchedulingCondition = testLock.newCondition();
    private boolean schedulingTriggered = false;
    private final AtomicInteger numSchedulerRuns = new AtomicInteger(0);


    public LlapTaskSchedulerServiceForTest(
        TaskSchedulerContext appClient, Clock clock) {
      super(appClient, clock, false);
    }

    @Override
    protected void schedulePendingTasks() {
      LOG.info("Attempted schedulPendingTasks");
      testLock.lock();
      try {
        if (controlScheduling.get()) {
          while (!schedulingTriggered) {
            try {
              triggerSchedulingCondition.await();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }
        numSchedulerRuns.incrementAndGet();
        super.schedulePendingTasks();
        schedulingTriggered = false;
        schedulingComplete = true;
        schedulingCompleteCondition.signal();
      } finally {
        testLock.unlock();
      }
    }

    // Enable or disable test scheduling control.
    void forTestsetControlScheduling(boolean control) {
      this.controlScheduling.set(control);
    }

    void forTestSignalSchedulingRun() throws InterruptedException {
      testLock.lock();
      try {
        schedulingTriggered = true;
        triggerSchedulingCondition.signal();
      } finally {
        testLock.unlock();
      }
    }

    boolean forTestAwaitSchedulingRun(long timeout) throws InterruptedException {
      testLock.lock();
      try {
        boolean success = true;
        while (!schedulingComplete) {
          if (timeout == -1) {
            schedulingCompleteCondition.await();
          } else {
            success = schedulingCompleteCondition.await(timeout, TimeUnit.MILLISECONDS);
            break;
          }
        }
        schedulingComplete = false;
        return success;
      } finally {
        testLock.unlock();
      }
    }

  }

  private static class LlapTaskSchedulerServiceForTestControlled extends LlapTaskSchedulerServiceForTest {

    private DelayedTaskSchedulerCallableControlled controlledTSCallable;

    public LlapTaskSchedulerServiceForTestControlled(
        TaskSchedulerContext appClient, Clock clock) {
      super(appClient, clock);
    }

    @Override
    LlapTaskSchedulerService.DelayedTaskSchedulerCallable createDelayedTaskSchedulerCallable() {
      controlledTSCallable = new DelayedTaskSchedulerCallableControlled();
      return controlledTSCallable;
    }

    class DelayedTaskSchedulerCallableControlled extends DelayedTaskSchedulerCallable {
      private final ReentrantLock lock = new ReentrantLock();
      private final Condition triggerRunCondition = lock.newCondition();
      private boolean shouldRun = false;
      private final Condition runCompleteCondition = lock.newCondition();
      private boolean runComplete = false;

      static final int STATE_NOT_RUN = 0;
      static final int STATE_NULL_FOUND = 1;
      static final int STATE_TIMEOUT_NOT_EXPIRED = 2;
      static final int STATE_RETURNED_TASK = 3;

      volatile int lastState = STATE_NOT_RUN;

      volatile boolean lastShouldScheduleTaskResult = false;
      volatile boolean shouldScheduleTaskTriggered = false;

      @Override
      public void processEvictedTask(TaskInfo taskInfo) {
        super.processEvictedTask(taskInfo);
        signalRunComplete();
      }

      @Override
      public TaskInfo getNextTask() throws InterruptedException {

        while (true) {
          lock.lock();
          try {
            while (!shouldRun) {
              triggerRunCondition.await();
            }
            // Preven subsequent runs until a new trigger is set.
            shouldRun = false;
          } finally {
            lock.unlock();
          }
          TaskInfo taskInfo = delayedTaskQueue.peek();
          if (taskInfo == null) {
            LOG.info("Triggered getTask but the queue is empty");
            lastState = STATE_NULL_FOUND;
            signalRunComplete();
            continue;
          }
          if (taskInfo.shouldDelayForLocality(
              LlapTaskSchedulerServiceForTestControlled.this.clock.getTime())) {
            LOG.info("Triggered getTask but the first element is not ready to execute");
            lastState = STATE_TIMEOUT_NOT_EXPIRED;
            signalRunComplete();
            continue;
          } else {
            delayedTaskQueue.poll(); // Remove the previously peeked element.
            lastState = STATE_RETURNED_TASK;
            return taskInfo;
          }
        }
      }

      @Override
      public boolean shouldScheduleTask(TaskInfo taskInfo) {
        shouldScheduleTaskTriggered = true;
        lastShouldScheduleTaskResult = super.shouldScheduleTask(taskInfo);
        return lastShouldScheduleTaskResult;
      }

      void resetShouldScheduleInformation() {
        shouldScheduleTaskTriggered = false;
        lastShouldScheduleTaskResult = false;
      }

      private void signalRunComplete() {
        lock.lock();
        try {
          runComplete = true;
          runCompleteCondition.signal();
        } finally {
          lock.unlock();
        }
      }

      void triggerGetNextTask() {
        lock.lock();
        try {
          shouldRun = true;
          triggerRunCondition.signal();
        } finally {
          lock.unlock();
        }
      }

      void awaitGetNextTaskProcessing() throws InterruptedException {
        lock.lock();
        try {
          while (!runComplete) {
            runCompleteCondition.await();
          }
          runComplete = false;
        } finally {
          lock.unlock();
        }
      }
    }
  }
}
