package org.apache.flink.fixpoint.util;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;

public enum ExecutionMode {

	BULK,
	INCREMENTAL,
	DELTA,
	COST_MODEL
	;
	
	public static ExecutionMode parseExecutionModeArgs(String input) throws InvalidArgumentException {
		if (input.equals("BULK")) {
			return BULK;
		}
		else if (input.equals("INCREMENTAL")) {
			return INCREMENTAL;
		}
		else if (input.equals("DELTA")) {
			return DELTA;
		}
		else if (input.equals("COST_MODEL")) {
			return COST_MODEL;
		}
		else {
			throw new InvalidArgumentException("Unkown Execution Type " + input);
		}
	}
} 
