package org.apache.flink.examples.java.preprocessing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class PreprocessEdges {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.err.println("Usage: PreprocessEdges <input-file-path> <output-file-path> ");
		}
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<Long, Long>> inEdges = env.readCsvFile(args[0]).fieldDelimiter('\t')
				.types(Long.class, Long.class);
		@SuppressWarnings("serial")
		DataSet<Tuple3<Long, Long, Double>> result = inEdges.flatMap(new FlatMapFunction<Tuple2<Long,Long>, 
				Tuple3<Long,Long, Double>>() {

					@Override
					public void flatMap(Tuple2<Long, Long> value,
							Collector<Tuple3<Long, Long, Double>> out)
							throws Exception {
						out.collect(new Tuple3<Long, Long, Double>(value.f0, value.f1, new Double(0.0)));
						out.collect(new Tuple3<Long, Long, Double>(value.f1, value.f0, new Double(0.0)));
					}
		});
		
		result.writeAsCsv(args[1], "\n", "\t");
		env.execute();
	}
}
