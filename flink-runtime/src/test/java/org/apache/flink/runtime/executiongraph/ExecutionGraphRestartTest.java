/*
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

package org.apache.flink.runtime.executiongraph;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getInstance;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getSimpleAcknowledgingTaskmanager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.tasks.NoOpInvokable;
import org.apache.flink.runtime.protocols.TaskOperationProtocol;
import org.junit.Test;

public class ExecutionGraphRestartTest {
	
	@Test
	public void testRestartManually() {
		final int NUM_TASKS = 31;
		
		try {
			TaskOperationProtocol tm = getSimpleAcknowledgingTaskmanager();
			Instance instance = getInstance(tm);
			
			Scheduler scheduler = new Scheduler();
			scheduler.newInstanceAvailable(instance);
			
			// The job:
			
			final AbstractJobVertex sender = new AbstractJobVertex("Task");
			sender.setInvokableClass(NoOpInvokable.class);
			sender.setParallelism(NUM_TASKS);
			
			final JobGraph jobGraph = new JobGraph("Pointwise Job", sender);
			
			ExecutionGraph eg = new ExecutionGraph(new JobID(), "test job", new Configuration());
			eg.setNumberOfRetriesLeft(0);
			eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());
			
			assertEquals(JobStatus.CREATED, eg.getState());
			
			eg.scheduleForExecution(scheduler);
			assertEquals(JobStatus.RUNNING, eg.getState());
			
			eg.getAllExecutionVertices().iterator().next().fail(new Exception("Test Exception"));
			assertEquals(JobStatus.FAILED, eg.getState());
			
			eg.restart();
			assertEquals(JobStatus.RUNNING, eg.getState());
			
			for (ExecutionVertex v : eg.getAllExecutionVertices()) {
				v.executionFinished();
			}
			assertEquals(JobStatus.FINISHED, eg.getState());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testRestartSelf() {
		final int NUM_TASKS = 31;
		
		try {
			TaskOperationProtocol tm = getSimpleAcknowledgingTaskmanager();
			Instance instance = getInstance(tm);
			
			Scheduler scheduler = new Scheduler();
			scheduler.newInstanceAvailable(instance);
			
			// The job:
			
			final AbstractJobVertex sender = new AbstractJobVertex("Task");
			sender.setInvokableClass(NoOpInvokable.class);
			sender.setParallelism(NUM_TASKS);
			
			final JobGraph jobGraph = new JobGraph("Pointwise Job", sender);
			
			ExecutionGraph eg = new ExecutionGraph(new JobID(), "test job", new Configuration());
			eg.setNumberOfRetriesLeft(1);
			eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());
			
			assertEquals(JobStatus.CREATED, eg.getState());
			
			eg.scheduleForExecution(scheduler);
			assertEquals(JobStatus.RUNNING, eg.getState());
			
			eg.getAllExecutionVertices().iterator().next().fail(new Exception("Test Exception"));
			
			// should have restarted itself
			assertEquals(JobStatus.RUNNING, eg.getState());
			
			for (ExecutionVertex v : eg.getAllExecutionVertices()) {
				v.executionFinished();
			}
			assertEquals(JobStatus.FINISHED, eg.getState());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
