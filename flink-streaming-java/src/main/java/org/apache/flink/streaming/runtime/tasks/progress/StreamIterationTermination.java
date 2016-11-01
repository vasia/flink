package org.apache.flink.streaming.runtime.tasks.progress;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.List;

public interface StreamIterationTermination extends Serializable {
	boolean terminate(List<Long> timeContext);
	void observeRecord(StreamRecord record);
	void observeWatermark(Watermark watermark);
}
