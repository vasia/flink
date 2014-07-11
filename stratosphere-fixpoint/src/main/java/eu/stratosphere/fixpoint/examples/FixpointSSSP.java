package eu.stratosphere.fixpoint.examples;

import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.fixpoint.api.FixedPointIteration;
import eu.stratosphere.fixpoint.api.StepFunction;
import eu.stratosphere.util.Collector;

public class FixpointSSSP implements ProgramDescription {

	public static void main(String... args) throws Exception {
		if (args.length < 4) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path> <max_iterations>");
			return;
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<Long, Long>> vertices = env.readCsvFile(args[0]).fieldDelimiter('\t').types(Long.class, Long.class);
		
		DataSet<Tuple3<Long, Long, Long>> edges = env.readCsvFile(args[1]).fieldDelimiter('\t').types(Long.class, Long.class, 
				Long.class); 
		
		int maxIterations = Integer.parseInt(args[3]);
		
		DataSet<Tuple2<Long, Long>> result = vertices.runOperation(FixedPointIteration.withWeightedDependencies(edges, 
				new ShortestPath(), maxIterations));

		result.print();
		env.execute("Fixed Point SSSP");
		
	}

	public static final class ShortestPath extends StepFunction<Long, Long, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public DataSet<Tuple2<Long, Long>> updateState(
				DataSet<Tuple4<Long, Long, Long, Long>> inNeighbors) {
			
			DataSet<Tuple2<Long, Long>> updatedDistances = inNeighbors.flatMap(new CandidateDistances())
															.groupBy(0).aggregate(Aggregations.MIN, 1)
															.project(0, 1).types(Long.class, Long.class);
			return updatedDistances;
		}
		
	}
	
	public static final class CandidateDistances extends FlatMapFunction<Tuple4<Long, Long, Long, Long>,
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
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations>";
	}
}
