package org.apache.flink.examples.java.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class PreprocessPartitions {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.err.println("Usage: PreprocessPartitions <input-file-path> <output-file-partitions>");
			System.exit(-1);
		}
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		@SuppressWarnings("serial")
		DataSet<Tuple2<Long, Long>> verticesWithPartition = env.readCsvFile(args[0]).fieldDelimiter('\t')
				.types(Long.class, Long.class).map(new MapFunction<Tuple2<Long, Long>, 
						Tuple2<Long, Long>>() {
							public Tuple2<Long, Long> map(Tuple2<Long, Long> value)
									throws Exception {					
								return new Tuple2<Long, Long>(value.f0, 1L);
							}
				});
		
		verticesWithPartition.writeAsCsv(args[1], "\n", "\t");
		env.execute();
	}
}
