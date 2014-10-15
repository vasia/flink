package org.apache.flink.examples.java.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 
 * Converts the distance weights [0, INF)
 * into similarity weights [0, 1]
 * by applying the function 1/(x+1) 
 *
 */
public class DistanceToSimilarity {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: DistanceToSimilarity <input-file-path> <output-file-path>");
			System.exit(-1);
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Long, Long, Double>> verticesWithDistance = env.readCsvFile(args[0])
				.fieldDelimiter('\t')
				.types(Long.class, Long.class, Double.class);
		
		DataSet<Tuple3<Long, Long, Double>> result = verticesWithDistance.map(
				new MapFunction<Tuple3<Long,Long,Double>, Tuple3<Long,Long,Double>>() {
					public Tuple3<Long, Long, Double> map(
							Tuple3<Long, Long, Double> value){
						return new Tuple3<Long, Long, Double>(value.f0, value.f1, 1.0 / (value.f2 + 1));
					}
		});
		
		result.writeAsCsv(args[1], "\n", "\t");
		
		env.execute();
	}
	
}
