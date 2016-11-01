/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.streaming.api.graph;


import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Internal
public class StreamScope implements Serializable {

	private List<Integer> contexts;
	private static transient int scopeIndex = 0;

	public StreamScope() {
		this(Collections.singleton(0));
	}
	
	public StreamScope(StreamScope scope){
		this(scope.contexts);
	}

	private StreamScope(Collection<Integer> contexts){
		this.contexts = new ArrayList<>();
		this.contexts.addAll(contexts);
	}
	
	public StreamScope nest(){
		StreamScope next = new StreamScope(this);
		next.contexts.add(++scopeIndex);
		return next;
	}
	
	public StreamScope unnest(){
		StreamScope next = new StreamScope(this);
		next.contexts.remove(contexts.size()-1);
		return next;
	}
	
	public int getContextForLevel(int level){
		if(level < 0 || level >= contexts.size()){
			throw new IllegalArgumentException("The context level provided is out of bounds.");
		}
		return contexts.get(level);
	}
	
	public int getCurrentLevelContext(){
		return contexts.get(contexts.size()-1);
	}
	
	public int getLevel(){
		return this.contexts.size()-1;
	}
	
	public boolean isInnerOf(StreamScope otherScope){
		if(this.getLevel() > otherScope.getLevel()){
			return otherScope.contexts.equals(this.contexts.subList(0, otherScope.getLevel()));
		}
		return false;
	}
	
	public boolean isOuterOf(StreamScope scope){
		return scope.isInnerOf(this);
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o){
			return true;
		}
		if (o == null || getClass() != o.getClass()){
			return false;
		}

		StreamScope that = (StreamScope) o;

		return contexts.equals(that.contexts);

	}

	@Override
	public int hashCode() {
		return contexts.hashCode();
	}
}




