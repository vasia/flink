package org.apache.flink.streaming.runtime.tasks.progress;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StructuredIterationTermination implements StreamIterationTermination {
	private final Map<List<Long>, Long> iterationIndices = new HashMap<>();
	private final long maxIterations;

	public StructuredIterationTermination(long maxIterations) {
		this.maxIterations = maxIterations;
	}

	public boolean terminate(List<Long> timeContext) {
		Long currentIndex = iterationIndices.get(timeContext);

		if(currentIndex == null || currentIndex < maxIterations-1) {
			return false;
		}
		return true;
	}

	public void observeRecord(StreamRecord record) {}

	public void observeWatermark(Watermark watermark) {
		if(watermark.getTimestamp() == Long.MAX_VALUE) {
			iterationIndices.remove(watermark.getContext());
		} else {
			iterationIndices.put(watermark.getContext(), watermark.getTimestamp());
		}
	}
}
