package org.apache.flink.fixpoint.examples;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.fixpoint.api.FixedPointIteration;
import org.apache.flink.fixpoint.api.StepFunction;


public class FixpointSimpleRageRank implements ProgramDescription {

	public static void main(String... args) throws Exception {
		
		if (args.length < 4) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path> <max_iterations>"
					+ " <execution_mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>");
			return;
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<Long, Double>> vertices = env.readCsvFile(args[0]).fieldDelimiter(' ').types(Long.class, Double.class);
		
		DataSet<Tuple3<Long, Long, Long>> edges = env.readCsvFile(args[1]).fieldDelimiter(' ').types(Long.class, Long.class, 
				Long.class); 
		
		int maxIterations = Integer.parseInt(args[3]);
		
		DataSet<Tuple2<Long, Double>> result = vertices.runOperation(FixedPointIteration.withWeightedDependencies(edges, 
				new UpdateRanks(), maxIterations, args[4]));

		result.print();
		env.execute("Fixed Point Simple PageRank");
	}
	
	@SuppressWarnings("serial")
	public static final class UpdateRanks extends StepFunction<Long, Double, Long> {

		@Override
		public DataSet<Tuple2<Long, Double>> updateState(
				DataSet<Tuple4<Long, Long, Double, Long>> inNeighbors) {
			
			DataSet<Tuple2<Long, Double>> newRanks = inNeighbors.map(new PartialRankMapper())
														.groupBy(0).aggregate(Aggregations.SUM, 1)
														.project(0, 1).types(Long.class, Double.class);
			return newRanks;
		}
		
		@Override 
		public DataSet<Tuple2<Long, Double>> deltaInput(DataSet<Tuple2<Long, Double>> bulkInput, 
				DataSet<Tuple2<Long, Double>> resultAfterBulk) {
					
			DataSet<Tuple2<Long, Double>> initialDeltas = bulkInput.join(resultAfterBulk).where(0).equalTo(0)
					.map(new MapFunction<Tuple2<Tuple2<Long, Double>,Tuple2<Long, Double>>, Tuple2<Long, Double>>() {

						public Tuple2<Long, Double> map(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value)
								throws Exception {
							return new Tuple2<Long, Double>(value.f0.f0, value.f1.f1 - value.f0.f1);
						}
					});
			
			return initialDeltas;
		}
		
		@Override
		public Tuple2<Long, Double> deltaUpdate(Tuple2<Long, Double> previousValue,	Tuple2<Long, Double> deltaValue) {
			return new Tuple2<Long, Double>(previousValue.f0, previousValue.f1 + deltaValue.f1);
		}
		
		@Override
		public boolean deltaEquals(Tuple2<Long, Double> previousValue,	Tuple2<Long, Double> deltaValue) {	
			return Math.abs(previousValue.f1 - deltaValue.f1) < 0.0001;
		}
		
	}
	
	public static final class PartialRankMapper extends MapFunction<Tuple4<Long, Long, Double, Long>, 
		Tuple2<Long, Double>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, Double> map(Tuple4<Long, Long, Double, Long> value)
				throws Exception {
			double partialRank = value.f2 / (double) value.f3;
			
			return new Tuple2<Long, Double>(value.f0, partialRank);
		}
		
	}
	
	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations> "
				+ "<execution_mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>";
	}

}
