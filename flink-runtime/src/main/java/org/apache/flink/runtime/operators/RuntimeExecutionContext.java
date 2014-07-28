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


package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.functions.ExecutionContext;
import org.apache.flink.runtime.execution.Environment;


/**
 * Default implementation of the {@link ExecutionContext} that delegates the calls to the nephele task
 * environment.
 *
 */
public class RuntimeExecutionContext implements ExecutionContext
{
	private final Environment env;

	public RuntimeExecutionContext(Environment env) {
		this.env = env;
	}


	@Override
	public String getTaskName() {
		return this.env.getTaskName();
	}


	@Override
	public int getNumberOfSubtasks() {
		return this.env.getCurrentNumberOfSubtasks();
	}


	@Override
	public int getSubtaskIndex() {
		return this.env.getIndexInSubtaskGroup() + 1;
	}
}
