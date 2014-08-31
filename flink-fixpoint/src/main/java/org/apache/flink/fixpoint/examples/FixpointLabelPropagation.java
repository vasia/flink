package org.apache.flink.fixpoint.examples;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
		
		if (args.length < 4) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path> <max_iterations> "
					+ "<execution_mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>");
			return;
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<Long, String>> vertices = env.readCsvFile(args[0]).fieldDelimiter('\t').types(Long.class, String.class);
		
		DataSet<Tuple3<Long, Long, Double>> edges = env.readCsvFile(args[1]).fieldDelimiter('\t').types(Long.class, Long.class, 
				Double.class); 
		
		int maxIterations = Integer.parseInt(args[3]);
	
		DataSet<Tuple2<Long, String>> result = vertices.runOperation(FixedPointIteration.withWeightedDependencies(edges, 
				new MostFrequentLabel(), maxIterations, args[4]));

		result.print();
		env.execute("Fixed Point Label Propagation");
		
	}
	
	@SuppressWarnings("serial")
	public static final class MostFrequentLabel extends StepFunction<Long, String, Double> {

		@Override
		public DataSet<Tuple2<Long, String>> updateState(
				DataSet<Tuple4<Long, Long, String, Double>> inNeighbors) {
			
			DataSet<Tuple2<Long, String>> updatedVertices = inNeighbors.flatMap(
					new FlatMapFunction<Tuple4<Long, Long, String, Double>, Tuple3<Long, String, Double>>() {
						public void flatMap(Tuple4<Long, Long, String, Double> value,
								Collector<Tuple3<Long, String, Double>> out)	throws Exception {
							if (!((value.f2).equals("n/a"))) {
								out.collect(new Tuple3<Long, String, Double>(value.f0, value.f2, value.f3));
							}
						}
			})
			.groupBy(0, 1).reduce(new ReduceFunction<Tuple3<Long, String, Double>>() {
				public Tuple3<Long, String, Double> reduce(Tuple3<Long, String, Double> value1,
						Tuple3<Long, String, Double> value2) throws Exception {
						return new Tuple3<Long, String, Double>(value1.f0, value1.f1, value1.f2 + value2.f2);
				}
			}).groupBy(0).reduce(new ReduceFunction<Tuple3<Long, String, Double>>() {
				public Tuple3<Long, String, Double> reduce(Tuple3<Long, String, Double> value1,
						Tuple3<Long, String, Double> value2) throws Exception {
					if (value1.f2 > value2.f2) {
						return value1;
					}
					else {
						return value2;
					}
				}	
			}).project(0, 1).types(Long.class, String.class);
					
			return updatedVertices;
		}
		
	}
	
	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations> "
				+ "<execution_mode (BULK / INCREMENTAL / DELTA / COST_MODEL (optional)>";
	}
	
}
