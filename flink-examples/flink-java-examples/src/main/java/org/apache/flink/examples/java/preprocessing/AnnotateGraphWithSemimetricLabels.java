package org.apache.flink.examples.java.preprocessing;

import java.util.Iterator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class AnnotateGraphWithSemimetricLabels {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		
		if (args.length < 3) {
			System.err.println("Usage: AnnotateGraphWithSemimetricLabels <initial-graph> <metric-backbone-graph>"
					+ " <output-annotated-graph> ");
		}
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<Long, Long>> inEdges = env.readCsvFile(args[0])
				.fieldDelimiter("\t").includeFields("110")
				.types(Long.class, Long.class);
		
		DataSet<Tuple3<Long, Long, Integer>> finalEdges = env.readCsvFile(args[1])
				.fieldDelimiter("\t").includeFields("1101")
				.types(Long.class, Long.class, Integer.class);

		DataSet<Tuple3<Long, Long, Boolean>> annotatedGraph = inEdges.coGroup(finalEdges)
				.where(0, 1).equalTo(0, 1).with(
						new CoGroupFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Integer>, 
							Tuple3<Long, Long, Boolean>>() {

							public void coGroup(Iterable<Tuple2<Long, Long>> initialEdge,
									Iterable<Tuple3<Long, Long, Integer>> metricEdge,
									Collector<Tuple3<Long, Long, Boolean>> out) {

								final Iterator<Tuple3<Long, Long, Integer>> metricEdges = metricEdge.iterator();
								final Iterator<Tuple2<Long, Long>> initialEdges = initialEdge.iterator();
								if (metricEdges.hasNext()) {
									Tuple3<Long, Long, Integer> finalEdge = metricEdges.next();

									if ((finalEdge.f2 == 1) || (finalEdge.f2 == 3)) {
										// metric
										out.collect(new Tuple3<Long, Long, Boolean>(
												finalEdge.f0, finalEdge.f1, false));
									}
									else {
										out.collect(new Tuple3<Long, Long, Boolean>(
												finalEdge.f0, finalEdge.f1, true));
									}
								}
								else {
									// the edge is semi-metric
									Tuple2<Long, Long> outEdge = initialEdges.next();
									out.collect(new Tuple3<Long, Long, Boolean>(
											outEdge.f0, outEdge.f1, true));
								}
							}
						}).withForwardedFieldsFirst("f0; f1");
		
		annotatedGraph.writeAsCsv(args[2], "\n", "\t");
		env.execute();
	}
}
