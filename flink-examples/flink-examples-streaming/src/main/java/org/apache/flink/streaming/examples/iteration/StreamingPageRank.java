package org.apache.flink.streaming.examples.iteration;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.FeedbackBuilder;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.LoopContext;
import org.apache.flink.streaming.api.functions.windowing.WindowLoopFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;
import java.util.*;

public class StreamingPageRank {
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	public static void main(String[] args) throws Exception {
		int numWindows = Integer.parseInt(args[0]);
		long windSize = Long.parseLong(args[1]);
		int parallelism = Integer.parseInt(args[2]);
		String outputDir = args[3];
		String inputDir = args.length > 4 ? args[4] : "";

		StreamingPageRank example = new StreamingPageRank(numWindows, windSize, parallelism, inputDir, outputDir);
		example.run();
	}


	/**
	 * TODO configure the windSize parameter and generalize the evaluation framework
	 *
	 * @param numWindows
	 * @param windSize
	 * @param parallelism
	 * @throws Exception
	 */
	public StreamingPageRank(int numWindows, long windSize, int parallelism, String inputDir, String outputDir) throws Exception {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(8);

		DataStream<Tuple2<Long, List<Long>>> inputStream = env.addSource(new PageRankSampleSrc());
		WindowedStream<Tuple2<Long, List<Long>>, Long, TimeWindow> winStream =

			inputStream.keyBy(new KeySelector<Tuple2<Long, List<Long>>, Long>() {
				@Override
				public Long getKey(Tuple2<Long, List<Long>> value) throws Exception {
					return value.f0;
				}
			}).timeWindow(Time.milliseconds(1000));
		
			winStream.iterateSyncFor(4,
				new MyWindowLoopFunction(),
				new MyFeedbackBuilder(),
				new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO))
			.print();
		env.getConfig().setExperimentConstants(numWindows, windSize, outputDir);
	}

	protected void run() throws Exception {
		System.err.println(env.getExecutionPlan());
		env.execute("Streaming Sync Iteration Example");
	}

	private static class MyFeedbackBuilder implements FeedbackBuilder<Tuple2<Long, Double>, Long> {
		@Override
		public KeyedStream<Tuple2<Long, Double>, Long> feedback(DataStream<Tuple2<Long, Double>> input) {
			return input.keyBy(new KeySelector<Tuple2<Long, Double>, Long>() {
				@Override
				public Long getKey(Tuple2<Long, Double> value) throws Exception {
					return value.f0;
				}
			});
		}
	}

//	private static class PageRankSource extends RichParallelSourceFunction<Tuple2<Long,List<Long>>> {
//		private int numberOfGraphs;
//
//		public PageRankSource(int numberOfGraphs) {
//			this.numberOfGraphs = numberOfGraphs;
//		}
//
//		@Override
//		public void run(SourceContext<Tuple2<Long, List<Long>>> ctx) {
//			int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
//			int parallelTask = getRuntimeContext().getIndexOfThisSubtask();
//
//			for(int i=0; i<numberOfGraphs; i++) {
//				for(Tuple2<Long,List<Long>> entry : getAdjacencyList()) {
//					if(entry.f0 % parallelism == parallelTask) {
//						ctx.collectWithTimestamp(entry, i);
//					}
//				}
//				ctx.emitWatermark(new Watermark(i));
//			}
//		}
//
//		@Override
//		public void cancel() {}

//		private List<Tuple2<Long,List<Long>>> getAdjacencyList() {
//			Map<Long,List<Long>> edges = new HashMap<>();
//			for(Object[] e : PageRankData.EDGES) {
//				List<Long> currentVertexEdges = edges.get((Long) e[0]);
//				if(currentVertexEdges == null) {
//					currentVertexEdges = new LinkedList<>();
//				}
//				currentVertexEdges.add((Long) e[1]);
//				edges.put((Long) e[0], currentVertexEdges);
//			}
//			List<Tuple2<Long,List<Long>>> input = new LinkedList<>();
//			for(Map.Entry<Long, List<Long>> entry : edges.entrySet()) {
//				input.add(new Tuple2(entry.getKey(), entry.getValue()));
//			}
//			return input;
//		}
//	}


	private static final List<Tuple3<Long, List<Long>, Long>> sampleStream = Lists.newArrayList(

		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(2l, 3l), 1000l),
		new Tuple3<>(3l, (List<Long>) Lists.newArrayList(1l), 1000l),
		new Tuple3<>(2l, (List<Long>) Lists.newArrayList(1l), 1000l),
		new Tuple3<>(2l, (List<Long>) Lists.newArrayList(1l, 3l), 2000l),
		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(2l), 2000l),
		new Tuple3<>(3l, (List<Long>) Lists.newArrayList(2l), 2000l),
		new Tuple3<>(3l, (List<Long>) Lists.newArrayList(2l, 1l), 3000l),
		new Tuple3<>(2l, (List<Long>) Lists.newArrayList(3l), 3000l),
		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(3l), 3000l),
		new Tuple3<>(4l, (List<Long>) Lists.newArrayList(1l), 4000l),
		new Tuple3<>(1l, (List<Long>) Lists.newArrayList(4l), 4000l)

	);


	private static class PageRankSampleSrc extends RichSourceFunction<Tuple2<Long, List<Long>>> {

		@Override
		public void run(SourceContext<Tuple2<Long, List<Long>>> ctx) throws Exception {
			long curTime = -1;
			for (Tuple3<Long, List<Long>, Long> next : sampleStream) {
				ctx.collectWithTimestamp(new Tuple2<>(next.f0, next.f1), next.f2);

				if (curTime == -1) {
					curTime = next.f2;
				}
				if (curTime < next.f2) {
					curTime = next.f2;
					ctx.emitWatermark(new Watermark(curTime - 1));

				}
			}
		}

		@Override
		public void cancel() {
		}
	}


	private static class PageRankFileSource extends RichParallelSourceFunction<Tuple2<Long, List<Long>>> {
		private int numberOfGraphs;
		private String directory;

		public PageRankFileSource(int numberOfGraphs, String directory) throws Exception {
			this.numberOfGraphs = numberOfGraphs;
			this.directory = directory;
		}

		@Override
		public void run(SourceContext<Tuple2<Long, List<Long>>> ctx) throws Exception {
			String path = directory + "/" + getRuntimeContext().getNumberOfParallelSubtasks() + "/part-" + getRuntimeContext().getIndexOfThisSubtask();
			for (int i = 0; i < numberOfGraphs; i++) {
				BufferedReader fileReader = new BufferedReader(new FileReader(path));
				String line;
				while ((line = fileReader.readLine()) != null) {
					String[] splitLine = line.split(" ");
					Long node = Long.parseLong(splitLine[0]);
					List<Long> neighbours = new LinkedList<>();
					for (int neighbouri = 1; neighbouri < splitLine.length; ++neighbouri) {
						neighbours.add(Long.parseLong(splitLine[neighbouri]));
					}
					ctx.collectWithTimestamp(new Tuple2<>(node, neighbours), i);
				}
				ctx.emitWatermark(new Watermark(i));
			}
		}

		@Override
		public void cancel() {
		}
	}

	private static class MyWindowLoopFunction implements WindowLoopFunction<Tuple2<Long, List<Long>>, Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>, Long, TimeWindow>, Serializable {
		Map<List<Long>, Map<Long, List<Long>>> neighboursPerContext = new HashMap<>();
		Map<List<Long>, Map<Long, Double>> pageRanksPerContext = new HashMap<>();

		public List<Long> getNeighbours(List<Long> timeContext, Long pageID) {
			return neighboursPerContext.get(timeContext).get(pageID);
		}

		@Override
		public void entry(LoopContext<Long> ctx, Iterable<Tuple2<Long, List<Long>>> iterable, Collector<Either<Tuple2<Long, Double>, Tuple2<Long, Double>>> collector) {
//			System.err.println("Entry Called for "+ ctx);
			Map<Long, List<Long>> adjacencyList = neighboursPerContext.get(ctx.getContext());
			if (adjacencyList == null) {
				adjacencyList = new HashMap<>();
				neighboursPerContext.put(ctx.getContext(), adjacencyList);
			}

			Map<Long, Double> pageRanks = pageRanksPerContext.get(ctx.getContext());
			if (pageRanks == null) {
				pageRanks = new HashMap<>();
				pageRanksPerContext.put(ctx.getContext(), pageRanks);
			}

			Tuple2<Long, List<Long>> next = iterable.iterator().next();
			adjacencyList.put(ctx.getKey(), next.f1);
			// save page rank to local state
			pageRanks.put(ctx.getKey(), 1.0);
//			}

			// send rank into feedback loop
			collector.collect(new Either.Left(new Tuple2<>(ctx.getKey(), 1.0)));

			System.err.println("ENTRY (" + ctx.getKey() + "):: " + Arrays.toString(ctx.getContext().toArray()) + " -> " + adjacencyList);
		}

		@Override
		public void step(LoopContext<Long> ctx, Iterable<Tuple2<Long, Double>> iterable, Collector<Either<Tuple2<Long, Double>, Tuple2<Long, Double>>> collector) {
			Map<Long, Double> summed = new HashMap<>();
			for (Tuple2<Long, Double> entry : iterable) {
				Double current = summed.get(entry.f0);
				if (current == null) {
					summed.put(entry.f0, entry.f1);
				} else {
					summed.put(entry.f0, current + entry.f1);
				}
			}

			for (Map.Entry<Long, Double> entry : summed.entrySet()) {
				List<Long> neighbourIDs = getNeighbours(ctx.getContext(), entry.getKey());
				Double currentRank = entry.getValue();

				// update current rank
				pageRanksPerContext.get(ctx.getContext()).put(entry.getKey(), currentRank);

				// generate new ranks for neighbours
				Double rankToDistribute = currentRank / (double) neighbourIDs.size();

				for (Long neighbourID : neighbourIDs) {
					collector.collect(new Either.Left(new Tuple2<>(neighbourID, rankToDistribute)));
				}
			}
			System.err.println("POST-STEP:: " + Arrays.toString(ctx.getContext().toArray()) + " (" + ctx.getSuperstep() + ")" + pageRanksPerContext.get(ctx.getContext()));
		}

		@Override
		public void onTermination(List<Long> timeContext, long superstep, Collector<Either<Tuple2<Long, Double>, Tuple2<Long, Double>>> out) {
			Map<Long, Double> vertexStates = pageRanksPerContext.get(timeContext);
			System.err.println("ON TERMINATION:: " + timeContext + "::" + vertexStates);
			if(vertexStates != null){
				for (Map.Entry<Long, Double> rank : pageRanksPerContext.get(timeContext).entrySet()) {
					out.collect(new Either.Right(new Tuple2(rank.getKey(), rank.getValue())));
				}	
			}
		}
	}
}
