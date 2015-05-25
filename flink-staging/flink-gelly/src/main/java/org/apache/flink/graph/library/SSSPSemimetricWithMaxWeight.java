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
public class SSSPSemimetricWithMaxWeight<K extends Comparable<K> & Serializable>
		implements GraphAlgorithm<K, Tuple2<Double, Double>, Tuple2<Double, Boolean>> {

	private final Integer maxIterations;

	public SSSPSemimetricWithMaxWeight(Integer maxIterations) {
		this.maxIterations = maxIterations;
	}

	@Override
	public Graph<K, Tuple2<Double, Double>, Tuple2<Double, Boolean>> run(Graph<K, Tuple2<Double, Double>, Tuple2<Double, Boolean>> input) {

		return input.runVertexCentricIteration(new VertexDistanceUpdater<K>(), new MinDistanceMessenger<K>(),
				maxIterations);
	}

	/**
	 * Function that updates the value of a vertex by picking the minimum
	 * distance from all incoming messages.
	 * 
	 * @param <K>
	 */
	public static final class VertexDistanceUpdater<K> extends VertexUpdateFunction<K, Tuple2<Double, Double>, Double> {

		@Override
		public void updateVertex(Vertex<K, Tuple2<Double, Double>> vertex,
				MessageIterator<Double> inMessages) {

			Double minDistance = Double.POSITIVE_INFINITY;

			for (double msg : inMessages) {
				if (msg < minDistance) {
					minDistance = msg;
				}
			}

			if (vertex.getValue().f0 > minDistance) {
				setNewVertexValue(new Tuple2<Double, Double>(minDistance, vertex.f1.f1));
			}
		}
	}

	/**
	 * Distributes the minimum distance associated with a given vertex among all
	 * the target vertices summed up with the edge's value.
	 * 
	 * @param <K>
	 */
	public static final class MinDistanceMessenger<K> extends MessagingFunction<K, Tuple2<Double, Double>, Double, Tuple2<Double, Boolean>> {

		public void sendMessages(Vertex<K, Tuple2<Double, Double>> vertex) {
			if (vertex.f1.f1 > -1) {	// otherwise it's not a valid source!
				for (Edge<K, Tuple2<Double, Boolean>> edge : getEdges()) {
					if (vertex.f1.f0 + edge.getValue().f0 < vertex.f1.f1) {
						// only propagate if the candidate distance might lead to a shorter path!
						sendMessageTo(edge.getTarget(), vertex.getValue().f0 + edge.getValue().f0);
					}
				}
			}
		}
	}
}