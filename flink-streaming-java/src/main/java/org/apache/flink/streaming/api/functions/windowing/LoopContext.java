package org.apache.flink.streaming.api.functions.windowing;

import java.util.List;

public class LoopContext<K> {
	
	final List<Long> context;
	final long superstep;
	final K key;

	public LoopContext(List<Long> context, long superstep, K key) {
		this.context = context;
		this.superstep = superstep;
		this.key = key;
	}

	public K getKey() {
		return key;
	}

	public List<Long> getContext() {
		return context;
	}

	public long getSuperstep() {
		return superstep;
	}

	@Override
	public String toString() {
		return super.toString()+" :: [ctx: "+ context +", step: "+ superstep +", key: "+key+"]";
	}
}
