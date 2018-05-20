package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.functions.windowing.LoopContext;
import org.apache.flink.streaming.api.functions.windowing.WindowLoopFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.types.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

@Internal
public class TwoWindowTerminateOperator<K, IN1, IN2, ACC1, ACC2, R, S, W1 extends Window, W2 extends Window>
	extends AbstractStreamOperator<Either<R,S>>
	implements TwoInputStreamOperator<IN1, IN2, Either<R,S>>, Serializable {

	public final static Logger logger = LoggerFactory.getLogger(TwoWindowTerminateOperator.class);
	private final KeySelector<IN1, K> entryKeying;
	private final KeySelector<IN2, K> feedbackKeying;
	
	WindowOperator<K, IN2, ACC2, Either<R,S>, W2> winOp2;
	WindowLoopFunction loopFunction;
	
	Set<List<Long>> activeIterations = new HashSet<>();
	StreamTask<?, ?> containingTask;

	TimestampedCollector<Either<R,S>> collector;

	//TODO implement this properly in a ProcessFunction and managed state
	Map<List<Long>, Map<K, List<IN1>>> entryBuffer;

	// MY METRICS
	private Map<List<Long>, Long> lastWinStartPerContext = new HashMap<>();
	private Map<List<Long>, Long> lastLocalEndPerContext = new HashMap<>();

	public TwoWindowTerminateOperator(KeySelector<IN1,K> entryKeySelector, KeySelector<IN2,K> feedbackKeySelector, WindowOperator winOp2, WindowLoopFunction loopFunction) {
		this.entryKeying = entryKeySelector;
		this.feedbackKeying = feedbackKeySelector;
		this.winOp2 = winOp2;
		this.loopFunction = loopFunction;
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Either<R,S>>> output) {
		super.setup(containingTask, config, output);

		// setup() both with own output
		StreamConfig config2 = new StreamConfig(config.getConfiguration().clone());
		config2.setOperatorName("WinOp2");
		winOp2.setup(containingTask, config2, output);
		this.containingTask = containingTask;
		this.entryBuffer = new HashMap<>();
	}

	@Override
	public final void open() throws Exception {
		collector = new TimestampedCollector<>(output);
		winOp2.getOperatorConfig().setStateKeySerializer(config.getStateKeySerializer(containingTask.getUserCodeClassLoader()));
		super.open();
		winOp2.open();
	}

	@Override
	public final void close() throws Exception {
		super.close();
		winOp2.close();
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		winOp2.dispose();
	}

	public void processElement1(StreamRecord<IN1> element) throws Exception {
		logger.info(getRuntimeContext().getIndexOfThisSubtask() +":: TWOWIN Received e from IN - "+ element);
		activeIterations.add(element.getContext());

		if(!entryBuffer.containsKey(element.getContext())){
			entryBuffer.put(element.getContext(), new HashMap<K, List<IN1>>());
		}
		
		Map<K, List<IN1>> tmp = entryBuffer.get(element.getContext());
		K key = entryKeying.getKey(element.getValue());
		if(!tmp.containsKey(key)){
			tmp.put(key, new ArrayList<IN1>());
		}
		tmp.get(key).add(element.getValue());
	}
	
	public void processElement2(StreamRecord<IN2> element) throws Exception {
		logger.info(getRuntimeContext().getIndexOfThisSubtask() +":: TWOWIN Received e from FEEDBACK - "+ element);
		winOp2.setCurrentKey(feedbackKeying.getKey(element.getValue()));
		if(activeIterations.contains(element.getContext())) {
			winOp2.processElement(element);
		}
	}
	
	public void processWatermark1(Watermark mark) throws Exception {
		logger.info(getRuntimeContext().getIndexOfThisSubtask() +":: TWOWIN Received from IN - "+ mark);
		lastWinStartPerContext.put(mark.getContext(), System.currentTimeMillis());
		if(entryBuffer.containsKey(mark.getContext())){
			for(Map.Entry<K, List<IN1>> entry : entryBuffer.get(mark.getContext()).entrySet()){
				collector.setAbsoluteTimestamp(mark.getContext(),0);
				loopFunction.entry(new LoopContext(mark.getContext(), 0, entry.getKey()), entry.getValue(), collector);
			}
			entryBuffer.remove(mark.getContext()); //entry is done for that context
			output.emitWatermark(mark);
		}
		lastLocalEndPerContext.put(mark.getContext(), System.currentTimeMillis());
	}
	
	public void processWatermark2(Watermark mark) throws Exception {
		logger.info(getRuntimeContext().getIndexOfThisSubtask() +":: TWOWIN Received from FEEDBACK - "+ mark);
		lastWinStartPerContext.put(mark.getContext(), System.currentTimeMillis());
		if(mark.iterationDone()) {
			activeIterations.remove(mark.getContext());
			if(mark.getContext().get(mark.getContext().size()-1) != Long.MAX_VALUE ) {
				loopFunction.onTermination(mark.getContext(), mark.getTimestamp(), collector);
			}
			winOp2.processWatermark(new Watermark(mark.getContext(), Long.MAX_VALUE, false, mark.iterationOnly()));
		} else {
			winOp2.processWatermark(mark);
		}
		lastLocalEndPerContext.put(mark.getContext(), System.currentTimeMillis());
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		winOp2.initializeState();
	}

	public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {}
	public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {}

	@Override
	public void sendMetrics(long windowEnd, List<Long> context) {
//		if (getContainingTask().getEnvironment().getExecutionConfig().isExperimentMetricsEnabled()) {
//			getContainingTask().getEnvironment().getJobManagerRef().tell(
//				new ProgressMetricsReport(getContainingTask().getEnvironment().getJobID(),
//					getOperatorConfig().getVertexID(),
//					getRuntimeContext().getIndexOfThisSubtask(),
//					context, lastWinStartPerContext.get(context), lastLocalEndPerContext.get(context), windowEnd
//				), ActorRef.noSender());
//		}
	}
}
