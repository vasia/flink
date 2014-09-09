package org.apache.flink.fixpoint.api;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.types.LongValue;


@SuppressWarnings("serial")
public class UpdatedElementsCostModelConvergence implements ConvergenceCriterion<LongValue> {

	private final int totalVertices;
	private final double avgNodeDegree;
	
	public UpdatedElementsCostModelConvergence(int numVertices, double avgDegree) {
		this.totalVertices = numVertices;
		this.avgNodeDegree = avgDegree;
	}

	/**
	 * value: updated elements during last iteration
	 * the cost model returns true if ( 3 * value / |SolutionSet| ) <= 1 / ( d + 1 )
	 * where d is the average node degree of the dependency graph
	 */
	@Override
	public boolean isConverged(int iteration, LongValue value) {		
//		System.out.println("[convergence check] Elements: " + value.getValue());
		double left_hand_side = ((double) (3*value.getValue()) / (double) this.totalVertices);
//		System.out.println("[convergence check] Left: " + left_hand_side);
		double right_hand_side = (double) (1.0 / (this.avgNodeDegree + 1.0));
//		System.out.println("[convergence check] Right: " + right_hand_side);
		return (left_hand_side <= right_hand_side);
	}

}