package org.apache.flink.examples.java.preprocessing;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

/**
 * 
 * Jaccard similarity for undirected graphs,
 * computed as the number of common neighbors between edges,
 * over their total number of neighbors. 
 *
 */
public class Jaccard {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: Jaccard <input-file-path> <output-file-path>");
			System.exit(-1);
		}
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<Long,Long>> edges = env.readCsvFile(args[0]).fieldDelimiter('\t')
				.types(Long.class, Long.class);
		
		// compute the total number of neighbors for every vertex
		DataSet<Tuple1<Long>> vertexIds = edges.flatMap(new FlatMapFunction<Tuple2<Long,Long>, 
				Tuple1<Long>>() {
					public void flatMap(Tuple2<Long, Long> value, Collector<Tuple1<Long>> out) {
						out.collect(new Tuple1<Long>(value.f0));
						out.collect(new Tuple1<Long>(value.f1));
					}
		}).distinct();
		
		DataSet<Tuple2<Long, Long>> verticesWithDegrees = vertexIds.join(edges).where(0).equalTo(0)
				.with(new FlatJoinWithCount()).groupBy(0).sum(1);
		
		// Compute the number of common neighbors for all edges
		DataSet<Tuple3<Long, Long, Long>> commonNeighborCandidates = edges.join(edges).where(1).equalTo(0)
				.projectFirst(0).projectSecond(1).projectFirst(1)
				.types(Long.class, Long.class, Long.class);
		
		// remove non-existing edges
		DataSet<Tuple3<Long, Long, Long>> commonNeighbors = commonNeighborCandidates.join(edges)
				.where(0, 1).equalTo(0, 1).projectFirst(0, 1, 2).types(Long.class, Long.class, Long.class);
		
		// <src, trg, common_neighbor_count>
		DataSet<Tuple3<Long, Long, Long>> edgesWithCounts = commonNeighbors.map(
				new AddOneCountMapper()).groupBy(0, 1).sum(2);
		
//		edgesWithCounts.print();
		
		// Compute the Jaccard similarity
		// attach the src's degree to the edge
		// <srcId, trgId, count, scrDegree>
		DataSet<Tuple4<Long, Long, Long, Long>> edgesWithSrcDegree = edgesWithCounts.join(verticesWithDegrees)
				.where(0).equalTo(0).projectFirst(0, 1, 2).projectSecond(1).types(Long.class, Long.class, Long.class, Long.class);
		
		DataSet<Tuple3<Long, Long, Double>> edgesWithJaccard = edgesWithSrcDegree.join(verticesWithDegrees)
				.where(1).equalTo(0).projectFirst(0, 1, 2, 3).projectSecond(1).types(Long.class, Long.class, 
						Long.class, Long.class, Long.class).map(new ComputeJaccardMapper());
							
		edgesWithJaccard.writeAsCsv(args[1], "\n", "\t");
		env.execute();
	}
	
	@SuppressWarnings("serial")
//	@ConstantFields("0")
	private static final class AddOneCountMapper implements MapFunction<Tuple3<Long,Long,Long>, 
		Tuple3<Long, Long, Long>> {
		public Tuple3<Long, Long, Long> map(Tuple3<Long, Long, Long> value) {
			return new Tuple3<Long, Long, Long>(value.f0, value.f2, 1L);
		}
	}
	
	@SuppressWarnings("serial")
	@ConstantFieldsFirst("0->0")
	private static final class FlatJoinWithCount implements FlatJoinFunction<Tuple1<Long>, 
		Tuple2<Long,Long>, Tuple2<Long, Long>> {
		public void join(Tuple1<Long> first, Tuple2<Long, Long> second,
				Collector<Tuple2<Long, Long>> out) {
			out.collect(new Tuple2<Long, Long>(first.f0, 1L));
		}
	}
	
	@SuppressWarnings("serial")
	@ConstantFields("0->0;1->1")
	private static final class ComputeJaccardMapper implements MapFunction<Tuple5<Long,Long,Long,Long,Long>, 
		Tuple3<Long, Long, Double>> {
		public Tuple3<Long, Long, Double> map(
				Tuple5<Long, Long, Long, Long, Long> value) {
			return new Tuple3<Long, Long, Double>(value.f0, value.f1, 
					((double)(value.f2)/(double)(value.f3 + value.f4)));
		}
	}
	
}
