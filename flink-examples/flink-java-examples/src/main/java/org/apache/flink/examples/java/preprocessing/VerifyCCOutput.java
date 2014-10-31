package org.apache.flink.examples.java.preprocessing;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class VerifyCCOutput {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 3) {
			System.err.println("Usage: PreprocessCCInput <input-file-1> <input-file-2> <output-file>");
			System.exit(-1);
		}
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<Long, Long>> input1 = env.readCsvFile(args[0]).fieldDelimiter('\t')
				.types(Long.class, Long.class);
		
		DataSet<Tuple2<Long, Long>> input2 = env.readCsvFile(args[1]).fieldDelimiter('\t')
				.types(Long.class, Long.class);
		
		@SuppressWarnings("serial")
		DataSet<Tuple2<Long, Long>> output = input1.join(input2).where(0).equalTo(0).with(
				new FlatJoinFunction<Tuple2<Long,Long>, Tuple2<Long,Long>, Tuple2<Long,Long>>() {

					@Override
					public void join(Tuple2<Long, Long> first, Tuple2<Long, Long> second,
							Collector<Tuple2<Long, Long>> out) throws Exception {
						if (first.f1 != second.f1) {
							out.collect(first);
							out.collect(second);
						}
					}
		});
		
		output.writeAsCsv(args[2], "\n", "\t");
		env.execute();
	}
}
