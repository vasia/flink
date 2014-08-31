package org.apache.flink.fixpoint.examples;

import org.apache.flink.fixpoint.api.FixedPointIteration;
import org.apache.flink.fixpoint.api.StepFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class FixpointSSSP implements ProgramDescription {

	public static void main(String... args) throws Exception {
		if (args.length < 4) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path> <max_iterations> "
					+ "<execution_mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>");
			return;
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<Long, Long>> vertices = env.readCsvFile(args[0]).fieldDelimiter('\t').types(Long.class, Long.class);
		
		DataSet<Tuple3<Long, Long, Long>> edges = env.readCsvFile(args[1]).fieldDelimiter('\t').types(Long.class, Long.class, 
				Long.class); 
		
		int maxIterations = Integer.parseInt(args[3]);
		
		DataSet<Tuple2<Long, Long>> result = vertices.runOperation(FixedPointIteration.withWeightedDependencies(edges, 
				new ShortestPath(), maxIterations, args[4]));

		result.print();
		env.execute("Fixed Point SSSP");
		
	}

	public static final class ShortestPath extends StepFunction<Long, Long, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public DataSet<Tuple2<Long, Long>> updateState(
				DataSet<Tuple4<Long, Long, Long, Long>> inNeighbors,
				DataSet<Tuple2<Long, Long>> state) {
			
			DataSet<Tuple2<Long, Long>> updatedDistances = inNeighbors.flatMap(new CandidateDistances())
															.groupBy(0).aggregate(Aggregations.MIN, 1)
															.project(0, 1).types(Long.class, Long.class);
			return updatedDistances;
		}
		
	}
	
	public static final class CandidateDistances implements FlatMapFunction<Tuple4<Long, Long, Long, Long>,
			Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple4<Long, Long, Long, Long> value,
				Collector<Tuple2<Long, Long>> out) throws Exception {
			if (value.f0 == value.f1) {
				// own value
				out.collect(new Tuple2<Long, Long>(value.f0, value.f2));
			}
			else {
				out.collect(new Tuple2<Long, Long>(value.f0, value.f2 + 1));
			}
			
		}
	
	}
	
	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations> "
				+ "<execution_mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>";
	}
}
