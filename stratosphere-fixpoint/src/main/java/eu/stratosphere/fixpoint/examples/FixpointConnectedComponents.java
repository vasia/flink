package eu.stratosphere.fixpoint.examples;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.operators.Operator;
import eu.stratosphere.api.java.operators.UnsortedGrouping;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.fixpoint.api.FixedPointIteration;

public class FixpointConnectedComponents {

	public static void main(String... args) throws Exception {
		
		if (args.length < 3) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path>");
			return;
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<Long, Long>> vertices = env.readCsvFile(args[0]).types(Long.class, Long.class);
		
		DataSet<Tuple2<Long, Long>> edges = env.readCsvFile(args[1]).fieldDelimiter('\t').types(Long.class, Long.class);
		
		FixedPointIteration<Long, Long, Tuple2<Long, Long>> cc = new FixedPointIteration<Long, Long, Tuple2<Long, Long>>(vertices, edges) {

					@Override
					public Operator<Tuple2<Long, Long>, ?> stepFunction(UnsortedGrouping<Tuple2<Long, Long>> neighborsValues) {
						return neighborsValues.aggregate(Aggregations.MIN, 1);
					}
		};
		
		cc.submit(args[2]);
	}
	
}
