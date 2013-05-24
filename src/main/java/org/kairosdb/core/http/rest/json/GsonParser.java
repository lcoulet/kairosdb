/*
 * Copyright 2013 Proofpoint Inc.
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

package org.kairosdb.core.http.rest.json;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.gson.*;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.google.inject.Inject;
import org.apache.bval.constraints.NotEmpty;
import org.apache.bval.jsr303.ApacheValidationProvider;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.aggregator.AggregatorFactory;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.groupby.GroupBy;
import org.kairosdb.core.groupby.GroupByFactory;
import org.kairosdb.core.http.rest.BeanValidationException;
import org.kairosdb.core.http.rest.QueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ConstraintViolation;
import javax.validation.Valid;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.constraints.NotNull;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.*;

import static com.google.common.base.Preconditions.checkState;


public class GsonParser
{
	private static final Logger logger = LoggerFactory.getLogger(GsonParser.class);

	private static final Validator VALIDATOR = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();

	private AggregatorFactory m_aggregatorFactory;
	private GroupByFactory m_groupByFactory;
	private Map<Class, Map<String, PropertyDescriptor>> m_descriptorMap;
	private final Object m_descriptorMapLock = new Object();
	private Gson m_gson;

	@Inject
	public GsonParser(AggregatorFactory aggregatorFactory, GroupByFactory groupByFactory)
	{
		m_aggregatorFactory = aggregatorFactory;
		m_groupByFactory = groupByFactory;

		m_descriptorMap = new HashMap<Class, Map<String, PropertyDescriptor>>();

		GsonBuilder builder = new GsonBuilder();
		builder.registerTypeAdapterFactory(new LowercaseEnumTypeAdapterFactory());
		builder.registerTypeAdapter(TimeUnit.class, new TimeUnitDeserializer());
		builder.registerTypeAdapter(Metric.class, new MetricDeserializer());

		m_gson = builder.create();
	}

	private PropertyDescriptor getPropertyDescriptor(Class objClass, String property) throws IntrospectionException
	{
		synchronized (m_descriptorMapLock)
		{
			Map<String, PropertyDescriptor> propMap = m_descriptorMap.get(objClass);

			if (propMap == null)
			{
				propMap = new HashMap<String, PropertyDescriptor>();
				m_descriptorMap.put(objClass, propMap);

				BeanInfo beanInfo = Introspector.getBeanInfo(objClass);
				PropertyDescriptor[] descriptors = beanInfo.getPropertyDescriptors();
				for (PropertyDescriptor descriptor : descriptors)
				{
					propMap.put(getUnderscorePropertyName(descriptor.getName()), descriptor);
				}
			}

			return (propMap.get(property));
		}
	}

	public static String getUnderscorePropertyName(String camelCaseName)
	{
		StringBuilder sb = new StringBuilder();

		for (char c : camelCaseName.toCharArray())
		{
			if (Character.isUpperCase(c))
				sb.append('_').append(Character.toLowerCase(c));
			else
				sb.append(c);
		}

		return (sb.toString());
	}

	private void validateObject(Object object) throws BeanValidationException
	{
		// validate object using the bean validation framework
		Set<ConstraintViolation<Object>> violations = VALIDATOR.validate(object);
		if (!violations.isEmpty())
		{
			throw new BeanValidationException(violations);
		}
	}

	public List<QueryMetric> parseQueryMetric(String json) throws QueryException, BeanValidationException
	{
		List<QueryMetric> ret = new ArrayList<QueryMetric>();

		JsonParser parser = new JsonParser();

		JsonObject obj = parser.parse(json).getAsJsonObject();

		Query query = m_gson.fromJson(obj, Query.class);

		validateObject(query);

		JsonArray metricsArray = obj.getAsJsonArray("metrics");
		for (int I = 0; I < metricsArray.size(); I++)
		{
			Metric metric = m_gson.fromJson(metricsArray.get(I), Metric.class);
			validateObject(metric);

			QueryMetric queryMetric = new QueryMetric(getStartTime(query), query.getCacheTime(),
					metric.getName());

			StringBuilder sb = new StringBuilder();
			sb.append(query.getCacheString()).append(metric.getCacheString());
			queryMetric.setCacheString(sb.toString());

			JsonObject jsMetric = metricsArray.get(I).getAsJsonObject();

			JsonElement aggregators = jsMetric.get("aggregators");
			if (aggregators != null)
				parseAggregators(queryMetric, aggregators.getAsJsonArray());

			long endTime = getEndTime(query);
			if (endTime > -1)
				queryMetric.setEndTime(endTime);

			JsonElement group_by = jsMetric.get("group_by");
			if (group_by != null)
			{
				JsonArray groupBys = group_by.getAsJsonArray();
				parseGroupBy(queryMetric, groupBys);
			}

			queryMetric.setTags(metric.getTags());

			ret.add(queryMetric);
		}

		return (ret);
	}

	private void parseAggregators(QueryMetric queryMetric, JsonArray aggregators) throws QueryException
	{
		for (int J = 0; J < aggregators.size(); J++)
		{
			JsonObject jsAggregator = aggregators.get(J).getAsJsonObject();

			String aggName = jsAggregator.get("name").getAsString();
			Aggregator aggregator = m_aggregatorFactory.createAggregator(aggName);
			checkState(aggregator != null, "Aggregator " + aggName + " could not be found.");

			//If it is a range aggregator we will default the start time to
			//the start of the query.
			if (aggregator instanceof RangeAggregator)
			{
				RangeAggregator ra = (RangeAggregator)aggregator;
				ra.setStartTime(queryMetric.getStartTime());
			}

			deserializeProperties(jsAggregator, aggName, aggregator);

			queryMetric.addAggregator(aggregator);
		}
	}

	private void parseGroupBy(QueryMetric queryMetric, JsonArray groupBys) throws QueryException
	{
		for (int J = 0; J < groupBys.size(); J++)
		{
			JsonObject jsGroupBy = groupBys.get(J).getAsJsonObject();

			String name = jsGroupBy.get("name").getAsString();
			GroupBy groupBy = m_groupByFactory.createGroupBy(name);
			checkState(groupBy != null, "GroupBy " + name + " could not be found.");

			deserializeProperties(jsGroupBy, name, groupBy);

			groupBy.setStartDate(queryMetric.getStartTime());

			queryMetric.addGroupBy(groupBy);
		}
	}

	private void deserializeProperties(JsonObject jsonObject, String name, Object object) throws QueryException
	{
		Set<Map.Entry<String, JsonElement>> props = jsonObject.entrySet();
		for (Map.Entry<String, JsonElement> prop : props)
		{
			String property = prop.getKey();
			if (property.equals("name"))
				continue;

			PropertyDescriptor pd = null;
			try
			{
				pd = getPropertyDescriptor(object.getClass(), property);
			}
			catch (IntrospectionException e)
			{
				logger.error("Introspection error on " + object.getClass(), e);
			}

			if (pd == null)
			{
				String msg = "Property '" + property + "' was specified for object '" + name +
						"' but no matching setter was found on '" + object.getClass() + "'";

				throw new QueryException(msg);
			}

			Class propClass = pd.getPropertyType();

			Object propValue = m_gson.fromJson(prop.getValue(), propClass);

			Method method = pd.getWriteMethod();
			if (method == null)
			{
				String msg = "Property '" + property + "' was specified for object '" + name +
						"' but no matching setter was found on '" + object.getClass().getName() + "'";

				throw new QueryException(msg);
			}

			try
			{
				method.invoke(object, propValue);
			}
			catch (Exception e)
			{
				logger.error("Invocation error: ", e);
				String msg = "Call to " + object.getClass().getName() + ":" + method.getName() +
						" failed with message: " + e.getMessage();

				throw new QueryException(msg);
			}
		}
	}

	private long getStartTime(Query request)
	{
		if (request.getStartAbsolute() != null)
			return Long.parseLong(request.getStartAbsolute());
		else
			return request.getStartRelative().getTimeRelativeTo(System.currentTimeMillis());
	}

	private long getEndTime(Query request)
	{
		if (request.getEndAbsolute() != null)
			return Long.parseLong(request.getEndAbsolute());
		else if (request.getEndRelative() != null)
			return request.getEndRelative().getTimeRelativeTo(System.currentTimeMillis());
		return -1;
	}

	//===========================================================================
	private static class Metric
	{
		@NotNull
		@NotEmpty()
		@SerializedName("name")
		private String name;

		@SerializedName("tags")
		private SetMultimap<String, String> tags;

		public Metric(String name, TreeMultimap<String, String> tags)
		{
			this.name = name;
			this.tags = tags;
		}

		public String getName()
		{
			return name;
		}

		public String getCacheString()
		{
			StringBuilder sb = new StringBuilder();

			sb.append(name).append(":");

			for (Map.Entry<String, String> tagEntry : tags.entries())
			{
				sb.append(tagEntry.getKey()).append("=");
				sb.append(tagEntry.getValue()).append(":");
			}

			return (sb.toString());
		}

		public SetMultimap<String, String> getTags()
		{
			if (tags != null)
			{
				return tags;
			}
			else
			{
				return HashMultimap.create();
			}
		}

	}

	//===========================================================================
	private static class Query
	{
		@SerializedName("start_absolute")
		private String m_startAbsolute;
		@SerializedName("end_absolute")
		private String m_endAbsolute;
		@SerializedName("cache_time")
		private int m_cacheTime;

		@Valid
		@SerializedName("start_relative")
		private RelativeTime m_startRelative;

		@Valid
		@SerializedName("end_relative")
		private RelativeTime m_endRelative;


		public String getStartAbsolute()
		{
			return m_startAbsolute;
		}

		public String getEndAbsolute()
		{
			return m_endAbsolute;
		}

		public int getCacheTime()
		{
			return m_cacheTime;
		}

		public RelativeTime getStartRelative()
		{
			return m_startRelative;
		}

		public RelativeTime getEndRelative()
		{
			return m_endRelative;
		}

		public String getCacheString()
		{
			StringBuilder sb = new StringBuilder();
			if (m_startAbsolute != null)
				sb.append(m_startAbsolute).append(":");

			if (m_startRelative != null)
				sb.append(m_startRelative.toString()).append(":");

			if (m_endAbsolute != null)
				sb.append(m_endAbsolute).append(":");

			if (m_endRelative != null)
				sb.append(m_endRelative.toString()).append(":");

			return (sb.toString());
		}

		@Override
		public String toString()
		{
			return "Query{" +
					"startAbsolute='" + m_startAbsolute + '\'' +
					", endAbsolute='" + m_endAbsolute + '\'' +
					", cacheTime=" + m_cacheTime +
					", startRelative=" + m_startRelative +
					", endRelative=" + m_endRelative +
					'}';
		}
	}

	//===========================================================================
	private static class LowercaseEnumTypeAdapterFactory implements TypeAdapterFactory
	{
		public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type)
		{
			Class<T> rawType = (Class<T>) type.getRawType();
			if (!rawType.isEnum())
			{
				return null;
			}

			final Map<String, T> lowercaseToConstant = new HashMap<String, T>();
			for (T constant : rawType.getEnumConstants())
			{
				lowercaseToConstant.put(toLowercase(constant), constant);
			}

			return new TypeAdapter<T>()
			{
				public void write(JsonWriter out, T value) throws IOException
				{
					if (value == null)
					{
						out.nullValue();
					}
					else
					{
						out.value(toLowercase(value));
					}
				}

				public T read(JsonReader reader) throws IOException
				{
					if (reader.peek() == JsonToken.NULL)
					{
						reader.nextNull();
						return null;
					}
					else
					{
						return lowercaseToConstant.get(reader.nextString());
					}
				}
			};
		}

		private String toLowercase(Object o)
		{
			return o.toString().toLowerCase(Locale.US);
		}
	}

	//===========================================================================
	private class TimeUnitDeserializer implements JsonDeserializer<TimeUnit>
	{
		public TimeUnit deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException
		{
			String unit = json.getAsString();
			TimeUnit tu;

			try
			{
				tu = TimeUnit.from(unit);
			}
			catch (IllegalArgumentException e)
			{
				throw new JsonSyntaxException(json.toString() + " is not a valid time unit, must be one of " +
						TimeUnit.toValueNames());
			}

			return tu;
		}
	}

	//===========================================================================
	private class MetricDeserializer implements JsonDeserializer<Metric>
	{
		@Override
		public Metric deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext)
				throws JsonParseException
		{
			JsonObject jsonObject = jsonElement.getAsJsonObject();

			String name = null;
			if (jsonObject.get("name") != null)
				name = jsonObject.get("name").getAsString();

			TreeMultimap<String, String> tags = TreeMultimap.create();
			JsonElement jeTags = jsonObject.get("tags");
			if (jeTags != null)
			{
				JsonObject joTags = jeTags.getAsJsonObject();
				for (Map.Entry<String, JsonElement> tagEntry : joTags.entrySet())
				{
					if (tagEntry.getKey().isEmpty())
						throw new JsonSyntaxException("Tag names cannot be empty");

					if (tagEntry.getValue().isJsonArray())
					{
						for (JsonElement element : tagEntry.getValue().getAsJsonArray())
						{
							if (element.isJsonNull() || element.getAsString().isEmpty())
								throw new JsonSyntaxException("Value for tag " + tagEntry.getKey() + " cannot be null or empty.");
							tags.put(tagEntry.getKey(), element.getAsString());
						}
					}
					else
					{
						if (tagEntry.getValue().isJsonNull() || tagEntry.getValue().getAsString().isEmpty())
							throw new JsonSyntaxException("Value for tag " + tagEntry.getKey() + " cannot be null or empty.");
						tags.put(tagEntry.getKey(), tagEntry.getValue().getAsString());
					}
				}
			}

			Metric ret = new Metric(name, tags);
			return (ret);
		}
	}

}
