package org.apache.flink.fixpoint.examples;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.fixpoint.api.FixedPointIteration;
import org.apache.flink.fixpoint.api.StepFunction;



public class FixpointConnectedComponents implements ProgramDescription {

	public static void main(String... args) throws Exception {
		
		if (args.length < 6) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path> <max_iterations> "
					+ "<execution_mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>"
					+ " <numParameters> <avg-node-degree>");
			return;
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<Long, Long>> vertices = env.readCsvFile(args[0]).fieldDelimiter('\t').types(Long.class, Long.class);
		
		DataSet<Tuple3<Long, Long, Long>> edges = env.readCsvFile(args[1]).fieldDelimiter('\t').types(Long.class, Long.class, 
				Long.class); 
		
		final int maxIterations = Integer.parseInt(args[3]);
		final int numParameters = Integer.parseInt(args[5]);
		final double avgNodeDegree = Double.parseDouble(args[6]);
	
		DataSet<Tuple2<Long, Long>> result = vertices.runOperation(FixedPointIteration.withWeightedDependencies(edges, 
				new MinId(), maxIterations, args[4], numParameters, avgNodeDegree));

		result.print();
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
