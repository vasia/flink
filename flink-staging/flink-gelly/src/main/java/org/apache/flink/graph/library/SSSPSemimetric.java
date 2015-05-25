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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

import java.io.Serializable;

@SuppressWarnings("serial")
public class SSSPSemimetric<K extends Comparable<K> & Serializable>
		implements GraphAlgorithm<K, Double, Tuple2<Double, Boolean>> {

	private final Integer maxIterations;

	public SSSPSemimetric(Integer maxIterations) {
		this.maxIterations = maxIterations;
	}

	@Override
	public Graph<K, Double, Tuple2<Double, Boolean>> run(Graph<K, Double, Tuple2<Double, Boolean>> input) {

		return input.runVertexCentricIteration(new VertexDistanceUpdater<K>(), new MinDistanceMessenger<K>(),
				maxIterations);
	}

	/**
	 * Function that updates the value of a vertex by picking the minimum
	 * distance from all incoming messages.
	 * 
	 * @param <K>
	 */
	public static final class VertexDistanceUpdater<K> extends VertexUpdateFunction<K, Double, Double> {

		@Override
		public void updateVertex(Vertex<K, Double> vertex,
				MessageIterator<Double> inMessages) {

			Double minDistance = Double.MAX_VALUE;

			for (double msg : inMessages) {
				if (msg < minDistance) {
					minDistance = msg;
				}
			}

			if (vertex.getValue() > minDistance) {
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
	public static final class MinDistanceMessenger<K> extends MessagingFunction<K, Double, Double, Tuple2<Double, Boolean>> {

		@Override
		public void sendMessages(Vertex<K, Double> vertex)
				throws Exception {
			for (Edge<K, Tuple2<Double, Boolean>> edge : getEdges()) {
				//TODO: only send a msg if the new distance is less than the max semi-metric weight
				sendMessageTo(edge.getTarget(), vertex.getValue() + edge.getValue().f0);
			}
		}
	}
}