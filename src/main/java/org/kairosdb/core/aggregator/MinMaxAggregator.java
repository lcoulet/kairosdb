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

import com.google.common.collect.Lists;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;

import java.util.Collections;
import java.util.Iterator;


/**
 * Converts all longs to double. This will cause a loss of precision for very large long values.
 */
@AggregatorName(name = "minmax", description = "Returns the minimum AND maximum data points for the time range.")
public class MinMaxAggregator extends RangeAggregator
{
	@Override
	protected RangeSubAggregator getSubAggregator()
	{
		return (new MinMaxDataPointAggregator());
	}

	private class MinMaxDataPointAggregator implements RangeSubAggregator
	{

		@Override
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			DataPoint minDataPoint = null;
                        DataPoint maxDataPoint = null;
                        if( dataPointRange.hasNext() ){
                            minDataPoint = dataPointRange.next();
                            maxDataPoint=minDataPoint;
                        }else{
                            return Collections.emptyList();
                        }
			while (dataPointRange.hasNext())
			{
				DataPoint dp = dataPointRange.next();

				if( dp.getDoubleValue() < minDataPoint.getDoubleValue() ){
                                    minDataPoint=dp;
                                }
                                if( dp.getDoubleValue() > maxDataPoint.getDoubleValue() ){
                                    maxDataPoint=dp;
                                }
                                
			}
                        if( minDataPoint == maxDataPoint){
                            return Collections.singletonList(minDataPoint);
                        }else{
                            DataPoint firstDataPoint = minDataPoint;
                            DataPoint secondDataPoint = maxDataPoint;
                            if( maxDataPoint.getTimestamp() < minDataPoint.getTimestamp()){
                                firstDataPoint = maxDataPoint;
                                secondDataPoint = minDataPoint;
                            }
                            return Lists.asList(firstDataPoint, secondDataPoint, new DataPoint[0]);
                        }
			
                       
		}
	}
}