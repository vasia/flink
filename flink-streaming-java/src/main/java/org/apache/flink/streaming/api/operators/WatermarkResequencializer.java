package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Iterator;


public class WatermarkResequencializer<IN>
	extends AbstractStreamOperator<IN>
	implements OneInputStreamOperator<IN, IN> {

	private Map<List<Long>,SortedSet<Watermark>> watermarksPerContext = new HashMap<>();
	private Map<List<Long>,Long> currentTimestampPerContext = new HashMap<>();

	@Override
	public void processElement(StreamRecord<IN> record) throws Exception {
		output.collect(record);
	}

	@Override
	public void processWatermark(Watermark watermark) {
		SortedSet<Watermark> watermarks = watermarksPerContext.get(watermark.getContext());
		if(watermarks == null) {
			watermarks = new TreeSet<>();
			watermarksPerContext.put(watermark.getContext(), watermarks);
		}
		watermarks.add(watermark);

		// move forward one timestamp if possible (otherwise watermark must wait until its time)
		Long current = currentTimestampPerContext.get(watermark.getContext());
		if (current == null || watermark.getTimestamp() == current + 1) {
			currentTimestampPerContext.put(watermark.getContext(), watermark.getTimestamp());
			for(Iterator<Watermark> it = watermarks.iterator(); it.hasNext();) {
				Watermark mark = it.next();
				if(mark.getTimestamp() <= watermark.getTimestamp()) {
					output.emitWatermark(mark);
					it.remove();
				}
			}
		}
	}
}
