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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.event.TaskEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class PriorityInputGateWrapper extends SingleInputGate {

	private static final Logger LOG = LoggerFactory.getLogger(PriorityInputGateWrapper.class);
	
	private final int priority;

	private final SingleInputGate wrappedInputGate;

	public PriorityInputGateWrapper(int priority, SingleInputGate wrapped) {
		super(wrapped.getOwningTaskName(), wrapped.getJobId(),wrapped.getConsumedResultId(), 
			wrapped.getConsumedPartitionType(), wrapped.getConsumedSubpartitionIndex(), 
			wrapped.getNumberOfInputChannels(), wrapped.getTaskActions(), null, 
			wrapped.isCreditBased());
		this.priority = priority;
		this.wrappedInputGate = wrapped;
	}

	public InputGate getWrappedInputGate() {
		return wrappedInputGate;
	}

	@Override
	public int getNumberOfInputChannels() {
		return wrappedInputGate.getNumberOfInputChannels();
	}

	@Override
	public boolean isFinished() {
		return wrappedInputGate.isFinished();
	}

	@Override
	public void requestPartitions() throws IOException, InterruptedException {
		wrappedInputGate.requestPartitions();
	}

	@Override
	public Optional<BufferOrEvent> getNextBufferOrEvent() throws IOException, InterruptedException {
		return wrappedInputGate.getNextBufferOrEvent();
	}

	@Override
	public Optional<BufferOrEvent> pollNextBufferOrEvent() throws IOException, InterruptedException {
		return wrappedInputGate.pollNextBufferOrEvent();
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		wrappedInputGate.sendTaskEvent(event);
	}

	@Override
	public void registerListener(InputGateListener listener) {
		wrappedInputGate.registerListener(new InputGateDelegator(listener));
	}

	@Override
	public int getPageSize() {
		return wrappedInputGate.getPageSize();
	}

	@Override
	public int getPriority() {
		return priority;
	}

	
	class InputGateDelegator implements InputGateListener{

		private final InputGateListener wrapped;
		
		public InputGateDelegator(InputGateListener listener){
			this.wrapped = listener;
		}

		@Override
		public void notifyInputGateNonEmpty(InputGate inputGate) {
			wrapped.notifyInputGateNonEmpty(PriorityInputGateWrapper.this);
		}
	}
	
}




