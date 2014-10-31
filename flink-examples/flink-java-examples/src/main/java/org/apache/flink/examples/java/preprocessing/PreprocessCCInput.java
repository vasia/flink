package org.apache.flink.examples.java.preprocessing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class PreprocessCCInput {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 3) {
			System.err.println("Usage: PreprocessCCInput <input-file-path> <output-file-vertices> <output-file-edges>");
			System.exit(-1);
		}
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		@SuppressWarnings("serial")
		DataSet<Tuple2<Long, Long>> edgesNoValue = env.readCsvFile(args[0]).fieldDelimiter('\t')
				.types(Long.class, Long.class, Double.class).map(new MapFunction<Tuple3<Long,Long, Double>, 
						Tuple2<Long, Long>>() {
							public Tuple2<Long, Long> map(Tuple3<Long, Long, Double> value)
									throws Exception {					
								return new Tuple2<Long, Long>(value.f0, value.f1);
							}
				});
		@SuppressWarnings("serial")
		DataSet<Tuple2<Long, Long>> verticesWithComponent = edgesNoValue.flatMap(new FlatMapFunction<
				Tuple2<Long,Long>, Tuple2<Long,Long>>() {

					public void flatMap(Tuple2<Long, Long> value,
							Collector<Tuple2<Long, Long>> out) throws Exception {
						out.collect(new Tuple2<Long, Long>(value.f0, value.f0));
						out.collect(new Tuple2<Long, Long>(value.f1, value.f1));
					}
		}).distinct();
		
		verticesWithComponent.writeAsCsv(args[1], "\n", "\t");
		edgesNoValue.writeAsCsv(args[2], "\n", "\t");
		env.execute();
	}
}
