package org.apache.flink.streaming.runtime.tasks.progress;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;

public class FixpointIterationTermination implements StreamIterationTermination {
	private Map<List<Long>, Boolean> convergedTracker = new HashMap<>();
	private Set<List<Long>> done = new HashSet<>();

	public boolean terminate(List<Long> timeContext) {
		return done.contains(timeContext);
	}

	public void observeRecord(StreamRecord record) {
		done.remove(record.getContext()); // if this partition is "back alive"
		convergedTracker.put(record.getContext(), false);
	}

	public void observeWatermark(Watermark watermark) {
		if(watermark.getTimestamp() == Long.MAX_VALUE) {
			// clean up
			convergedTracker.remove(watermark.getContext());
			done.remove(watermark.getContext());
		} else {
			Boolean converged = convergedTracker.get(watermark.getContext());
			if(converged != null && converged) {
				done.add(watermark.getContext());
			} else {
				convergedTracker.put(watermark.getContext(), true);
			}
		}
	}
}
