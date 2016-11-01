package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class WindowedStreamWatermarkFiller<IN>
	extends AbstractStreamOperator<IN>
	implements OneInputStreamOperator<IN, IN> {

	private static final long serialVersionUID = 1L;

	private Map<List<Long>,Map<Long,List<StreamRecord<IN>>>> recordsByContext = new HashMap<>();
	private long currentTimestamp = 0;

	public WindowedStreamWatermarkFiller() {
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		if(element.getContext().get(0) > 0) {
			System.out.println(element);
		}
		Map<Long, List<StreamRecord<IN>>> elements = getElements(element.getContext());

		List<StreamRecord<IN>> elementsWithSameTimestamp = elements.get(element.getTimestamp());
		if(elementsWithSameTimestamp == null) {
			elementsWithSameTimestamp = new LinkedList<>();
			elements.put(element.getTimestamp(), elementsWithSameTimestamp);
		}
		elementsWithSameTimestamp.add(element);
	}

	@Override
	public void processWatermark(Watermark watermark) {
		Map<Long,List<StreamRecord<IN>>> elements = getElements(watermark.getContext());

		for(long i=currentTimestamp; i<=watermark.getTimestamp(); i++) {
			if(elements.get(i) != null) {
				boolean iterationOnly = false;
				if(currentTimestamp < watermark.getTimestamp()) {
					iterationOnly = true;
				}
				for(StreamRecord<IN> record : elements.get(i)) {
					output.collect(record);
				}
				//System.out.println("Filler: " + new Watermark(watermark.getContext(), i, false, iterationOnly));
				output.emitWatermark(new Watermark(watermark.getContext(), i, false, iterationOnly));
				elements.remove(i);
			}
		}
/*		for(Iterator<Map.Entry<Long,List<StreamRecord<IN>>>> it = elements.entrySet().iterator(); it.hasNext();) {
			Map.Entry<Long,List<StreamRecord<IN>>> current = it.next();
			if(current.getKey() > watermark.getTimestamp()) continue;

			for(StreamRecord<IN> record : current.getValue()) {
				output.collect(record);
			}
			if(current.getKey() != watermark.getTimestamp()) {
				output.emitWatermark(new Watermark(watermark.getContext(), current.getKey(), true));
			}
			it.remove();
		}*/
		//output.emitWatermark(watermark);
	}

	private Map<Long, List<StreamRecord<IN>>> getElements(List<Long> context) {
		Map<Long, List<StreamRecord<IN>>> elements = recordsByContext.get(context);
		if(elements == null) {
			elements = new HashMap();
			recordsByContext.put(context, elements);
		}
		return elements;
	}
}
