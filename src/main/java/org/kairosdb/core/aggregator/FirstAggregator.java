/*
 * Copyright 2013 Integral Systems Europe.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.core.aggregator;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;

import java.util.Collections;
import java.util.Iterator;



@AggregatorName(name = "first", description = "Returns the first data point for the time range.")
public class FirstAggregator extends RangeAggregator
{
	@Override
	protected RangeSubAggregator getSubAggregator()
	{
		return (new FirstDataPointAggregator());
	}

	private class FirstDataPointAggregator implements RangeSubAggregator
	{

		@Override
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			if (dataPointRange.hasNext())
			{	
                                DataPoint dp = dataPointRange.next();
                                
                                while(dataPointRange.hasNext()){dataPointRange.next();} 
				
                                return Collections.singletonList(dp);
			}
                        return Collections.emptyList();			                        
		}
	}
}