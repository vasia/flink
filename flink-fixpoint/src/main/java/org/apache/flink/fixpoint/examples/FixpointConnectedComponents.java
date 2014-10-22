package org.apache.flink.fixpoint.examples;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.fixpoint.api.FixedPointIteration;
import org.apache.flink.fixpoint.api.StepFunction;
import org.apache.flink.fixpoint.util.ExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;



public class FixpointConnectedComponents implements ProgramDescription {

	public static void main(String... args) throws Exception {
		
		if (args.length < 7) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path> <max_iterations> "
					+ " <numParameters> <avg-node-degree>"
					+ " <execution_mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>");
			return;
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<Long, Long>> vertices = env.readCsvFile(args[0]).types(Long.class)
				.map(new MapFunction<Tuple1<Long>, Tuple2<Long, Long>>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<Long, Long> map(Tuple1<Long> value)
							throws Exception {
						return new Tuple2<Long, Long>(value.f0, value.f0);
					}
				});
		
		DataSet<Tuple3<Long, Long, Long>> edges = env.readCsvFile(args[1]).fieldDelimiter('\t').types(Long.class, Long.class)
				.map(new MapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>>() {
					private static final long serialVersionUID = 1L;

					public Tuple3<Long, Long, Long> map(Tuple2<Long, Long> value)
							throws Exception {
						return new Tuple3<Long, Long, Long>(value.f0, value.f1, 1L);
					}
					
				});
		
		final int maxIterations = Integer.parseInt(args[3]);
		final int numParameters = Integer.parseInt(args[4]);
		final double avgNodeDegree = Double.parseDouble(args[5]);
	
		DataSet<Tuple2<Long, Long>> result = vertices.runOperation(FixedPointIteration.withWeightedDependencies(edges, 
				new MinId(), maxIterations, ExecutionMode.parseExecutionModeArgs(args[6]), 
				numParameters, avgNodeDegree));

		result.writeAsText(args[2]);
		env.execute("Fixed Point Connected Components");
		
	}
	
	@SuppressWarnings("serial")
	public static final class MinId extends StepFunction<Long, Long, Long> {

		@Override
		public DataSet<Tuple2<Long, Long>> updateState(
				DataSet<Tuple4<Long, Long, Long, Long>> inNeighbors, DataSet<Tuple2<Long, Long>> state) {
			
			DataSet<Tuple3<Long, Long, Long>> groupedNeighbors = inNeighbors.groupBy(0).aggregate(Aggregations.MIN, 2)
																	.project(0, 2, 3)
																	.types(Long.class, Long.class, Long.class);
			return groupedNeighbors.project(0, 1).types(Long.class, Long.class);
		}
		
	}
	
	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations> "
				+ "<execution_mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>"
				+ " <numParameters> <avg-node-degree>";
	}
	
}
