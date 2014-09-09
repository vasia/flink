package org.apache.flink.fixpoint.examples;

import java.util.Random;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.fixpoint.api.FixedPointIteration;
import org.apache.flink.fixpoint.api.StepFunction;
import org.apache.flink.util.Collector;



public class FixpointLabelPropagation implements ProgramDescription {

	public static void main(String... args) throws Exception {
		
		if (args.length < 7) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path> <number-of-labels> "
					+ "<max-iterations> <execution-mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>"
					+ "<numParameters> <avg-node-degree>");
			return;
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		final int numLabels = Integer.parseInt(args[3]);
		final int maxIterations = Integer.parseInt(args[4]);
		final int numParameters = Integer.parseInt(args[6]);
		final double avgNodeDegree = Double.parseDouble(args[7]);
		
		// initialize vertices with random labels
		@SuppressWarnings("serial")
		DataSet<Tuple2<Long, Integer>> vertices = env.readTextFile(args[0]).map(
				new MapFunction<String, Tuple2<Long, Integer>>() {
					public Tuple2<Long, Integer> map(String value)
							throws Exception {
						Random randomGenerator = new Random();
						return new Tuple2<Long, Integer>(Long.parseLong(value), randomGenerator.nextInt(numLabels));
					}
				}) ;
		
		DataSet<Tuple3<Long, Long, Double>> edges = env.readCsvFile(args[1]).fieldDelimiter('\t').types(Long.class, Long.class, 
				Double.class); 
	
		DataSet<Tuple2<Long, Integer>> result = vertices.runOperation(FixedPointIteration.withWeightedDependencies(edges, 
				new MostFrequentLabel(), maxIterations, args[5], numParameters, avgNodeDegree));

		result.print();
		env.execute("Fixed Point Label Propagation");
		
	}
	
	@SuppressWarnings("serial")
	public static final class MostFrequentLabel extends StepFunction<Long, Integer, Double> {

		@Override
		public DataSet<Tuple2<Long, Integer>> updateState(
				DataSet<Tuple4<Long, Long, Integer, Double>> inNeighbors,
				DataSet<Tuple2<Long, Integer>> state) {
			
			DataSet<Tuple2<Long, Integer>> updatedVertices = inNeighbors.flatMap(
					new FlatMapFunction<Tuple4<Long, Long, Integer, Double>, Tuple3<Long, Integer, Double>>() {
						public void flatMap(Tuple4<Long, Long, Integer, Double> value,
								Collector<Tuple3<Long, Integer, Double>> out)	throws Exception {
																		// <trgId, candidateLabel, weight>
								out.collect(new Tuple3<Long, Integer, Double>(value.f0, value.f2, value.f3));
						}
			})
			// groupBy <vertexID, label> 
			.groupBy(0, 1).reduce(new ReduceFunction<Tuple3<Long, Integer, Double>>() {
				public Tuple3<Long, Integer, Double> reduce(Tuple3<Long, Integer, Double> value1,
						Tuple3<Long, Integer, Double> value2) throws Exception {
																// accumulate weights
						return new Tuple3<Long, Integer, Double>(value1.f0, value1.f1, value1.f2 + value2.f2);
				}
				// groupBy vertexID
			}).groupBy(0).reduce(new ReduceFunction<Tuple3<Long, Integer, Double>>() {
				public Tuple3<Long, Integer, Double> reduce(Tuple3<Long, Integer, Double> value1,
						Tuple3<Long, Integer, Double> value2) throws Exception {
					// choose label with highest total weight
					if (value1.f2 > value2.f2) {
						return value1;
					}
					else {
						return value2;
					}
				}	
			}).project(0, 1).types(Long.class, Integer.class);
					
			return updatedVertices;
		}
		
	}
	
	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <number-of-labels> <max-number-of-iterations> "
				+ "<execution_mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>"
				+ "<numParameters> <avg-node-degree>";
	}
	
}
