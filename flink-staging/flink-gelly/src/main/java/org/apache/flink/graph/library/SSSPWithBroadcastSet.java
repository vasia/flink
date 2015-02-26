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

package org.apache.flink.graph.library;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

import java.io.Serializable;
import java.util.Collection;

@SuppressWarnings("serial")
public class SSSPWithBroadcastSet<K extends Comparable<K> & Serializable>
		implements GraphAlgorithm<K, Double, Double> {

	private final DataSet<K> srcVertexId;
	private final Integer maxIterations;
	private final ExecutionEnvironment env;

	public SSSPWithBroadcastSet(DataSet<K> srcVertexId, Integer maxIterations, ExecutionEnvironment env) {
		this.srcVertexId = srcVertexId;
		this.maxIterations = maxIterations;
		this.env = env;
	}

	@Override
	public Graph<K, Double, Double> run(Graph<K, Double, Double> input) {

		DataSet<Vertex<K, Double>> initVertices = input.getVertices().map(
				new InitVerticesMapper<K>()).withBroadcastSet(srcVertexId, "source");
		
		Graph<K, Double, Double> initGraph = Graph.fromDataSet(initVertices, input.getEdges(), env);
		
		return initGraph.runVertexCentricIteration(new VertexDistanceUpdater<K>(),
						new MinDistanceMessenger<K>(), maxIterations);
	}

	public static final class InitVerticesMapper<K extends Comparable<K> & Serializable>
			extends RichMapFunction<Vertex<K, Double>, Vertex<K, Double>> {

		private long srcVertexId;

		@Override
		public void open(Configuration parameters) throws Exception {
			Collection<Long> sourceIds = getRuntimeContext().getBroadcastVariable("source");
			srcVertexId = sourceIds.iterator().next();
		}
		
		public Vertex<K, Double> map(Vertex<K, Double> value) {
			if (value.f0.equals(srcVertexId)) {
				return new Vertex<K, Double>(value.getId(), 0.0);
			} else {
				return new Vertex<K, Double>(value.getId(), Double.MAX_VALUE);
			}
		}
	}

	/**
	 * Function that updates the value of a vertex by picking the minimum
	 * distance from all incoming messages.
	 * 
	 * @param <K>
	 */
	public static final class VertexDistanceUpdater<K extends Comparable<K> & Serializable>
			extends VertexUpdateFunction<K, Double, Double> {

		@Override
		public void updateVertex(K vertexKey, Double vertexValue,
				MessageIterator<Double> inMessages) {

			Double minDistance = Double.MAX_VALUE;

			for (double msg : inMessages) {
				if (msg < minDistance) {
					minDistance = msg;
				}
			}

			if (vertexValue > minDistance) {
				setNewVertexValue(minDistance);
			}
		}
	}

	/**
	 * Distributes the minimum distance associated with a given vertex among all
	 * the target vertices summed up with the edge's value.
	 * 
	 * @param <K>
	 */
	public static final class MinDistanceMessenger<K extends Comparable<K> & Serializable>
			extends MessagingFunction<K, Double, Double, Double> {

		@Override
		public void sendMessages(K vertexKey, Double newDistance)
				throws Exception {
			for (Edge<K, Double> edge : getOutgoingEdges()) {
				sendMessageTo(edge.getTarget(), newDistance + edge.getValue());
			}
		}
	}
}