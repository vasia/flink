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

package org.apache.flink.graph.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.SSSPWithBroadcastSet;
import org.apache.flink.util.Collector;

public class SemimetricSSSP implements ProgramDescription {

	/**the total number of sources given as input **/
	private static long totalNumberOfSources;

	/** the number of sources for which relevance search is run in parallel, in every step **/
	private static int sourcesPerStep;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (args.length < 4) {
			System.err.println("Usage: Semi-mtric SSSP <input-edges> <output-path> <total-sources> <sources-per-step>");
		}

		totalNumberOfSources = Long.parseLong(args[2]);
		sourcesPerStep = Integer.parseInt(args[3]);

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// read the edges and create a dataset of sources
		DataSet<Tuple4<Long, Long, Double, String>> inputEdges = env.readCsvFile(args[0])
				.fieldDelimiter("\t").lineDelimiter("\n")
				.types(Long.class, Long.class, Double.class, String.class);

		// get the unlabeled edge source IDs in a list 
		// (the graph is undirected, so we only need to consider the source IDs)
		DataSet<Tuple1<Long>> unlabeledIds = inputEdges.flatMap(
				new FlatMapFunction<Tuple4<Long, Long, Double, String>, Tuple1<Long>>() {

					public void flatMap(Tuple4<Long, Long, Double, String> edge,
							Collector<Tuple1<Long>> out) {
						if (edge.f3.equals("false")) { // unlabeled edge
							out.collect(new Tuple1<Long>(edge.f0));
						}
					}
		});
		
		/** assign an index to each source id **/
		DataSet<Tuple2<Long, Integer>> sourceIdsWithIndex = unlabeledIds.reduceGroup(
				new AssignIndexToEachSourceIDReducer());
		
		// get the edges and create the graph
		DataSet<Edge<Long, Double>> edges = inputEdges.map(
				new MapFunction<Tuple4<Long, Long, Double, String>, Edge<Long, Double>>() {

					public Edge<Long, Double> map(Tuple4<Long, Long, Double, String> value) {
						return new Edge<Long, Double>(value.f0, value.f1, value.f2);
					}
		});

		Graph<Long, Double, Double> graph = Graph.fromDataSet(edges, 
				new MapFunction<Long, Double>() {

					public Double map(Long value) throws Exception {
						return Double.MAX_VALUE;
					}
		}, env);

		/* 
		 * ********		Run the outer iterative loop of the application, 	********
		 * ********		i.e. schedule a batch of SSSPs of _sourcesPerStep_	********
		 * ******** 	at a time											******** 
		 */
		for (int i=0; i < totalNumberOfSources; i += sourcesPerStep) {
			doSSSP(graph, sourceIdsWithIndex, i, env, args[1]);
		}
	}

	private static void doSSSP(Graph<Long, Double, Double> input, DataSet<Tuple2<Long, Integer>> sourcesWithIndex, 
			int index, ExecutionEnvironment env, String outputPath) throws Exception {
		for (int i = index; ((i < index + sourcesPerStep) && (i < totalNumberOfSources)); i++) {
			DataSet<Long> sourceId = sourcesWithIndex.flatMap(new SelectSource(i));

			input.run(new SSSPWithBroadcastSet<Long>(sourceId, 50, env)).getVertices().first(100)
				.writeAsCsv(outputPath + "/" + i, "\n", "\t");
		}
		env.execute();
	}
	
	@SuppressWarnings("serial")
	private static final class SelectSource implements FlatMapFunction<Tuple2<Long, Integer>, Long> {

		private final int srcIndex;

		public SelectSource(int sourceIndex) {
			this.srcIndex = sourceIndex;
		}

		@Override
		public void flatMap(Tuple2<Long, Integer> value, Collector<Long> out) {
			if (value.f1.intValue() == srcIndex) {
				out.collect(value.f0);
			}
		}
	}

	@Override
	public String getDescription() {
		return "Semi-metric SSSP";
	}

	/** 
	 * Assigns a numeric index to each of the given input sources.
	 * This index is used to define which sources participate if each step.
	 */
	@SuppressWarnings("serial")
	private static final class AssignIndexToEachSourceIDReducer implements GroupReduceFunction<
		Tuple1<Long>, Tuple2<Long, Integer>> {
		
		public void reduce(Iterable<Tuple1<Long>> values, Collector<Tuple2<Long, Integer>> out) {
			int index = 0;
			for (Tuple1<Long> value : values) {
				out.collect(new Tuple2<Long, Integer>(value.f0, index));
				index++;
			}
		}
	}
}
