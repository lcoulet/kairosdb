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

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.stream.MalformedJsonException;
import org.junit.Test;
import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.util.ValidationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class JsonMetricParserTest
{
	@Test
	public void test_nullMetricName_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"datapoints\": [[1,2]], \"tags\":{\"foo\":\"bar\"}}, {\"datapoints\": [[1,2]], \"tags\":{\"foo\":\"bar\"}}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");

		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		try
		{
			parser.parse();
			fail("Should throw ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("metric[1].name may not be null."));
		}
	}

	@Test
	public void test_timestampButNoValue_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"timestamp\": 1234}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		try
		{
			parser.parse();
			fail("Should throw ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("metric[0].value cannot be null or empty."));
		}
	}

	@Test
	public void test_valueButNoTimestamp_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metric1\", \"value\": 1234}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		try
		{
			parser.parse();
			fail("Should throw ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("metric[0].timestamp must be greater than 0."));
		}
	}

	@Test
	public void test_emptyMetricName_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		try
		{
			parser.parse();
			fail("Should throw ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("metric[0].name may not be empty."));
		}
	}

	@Test
	public void test_metricName_invalidCharacters() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"bad:name\", \"tags\":{\"foo\":\"bar\"}, \"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		try
		{
			parser.parse();
			fail("Should throw ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("metric[0].name may only contain alphanumeric characters plus periods '.', slash '/', dash '-', and underscore '_'."));
		}
	}

	@Test
	public void test_emptyTags_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metricName\", \"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		try
		{
			parser.parse();
			fail("Should throw ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("metric[0].tags cannot be null or empty."));
		}
	}

	@Test
	public void test_emptyTagName_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metricName\", \"tags\":{\"\":\"bar\"}, \"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		try
		{
			parser.parse();
			fail("Should throw ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("metric[0].tag[0].name may not be empty."));
		}
	}

	@Test
	public void test_tagName_invalidCharacters() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metricName\", \"tags\":{\"bad:name\":\"bar\"}, \"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		try
		{
			parser.parse();
			fail("Should throw ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("metric[0].tag[0].name may only contain alphanumeric characters plus periods '.', slash '/', dash '-', and underscore '_'."));
		}
	}

	@Test
	public void test_emptyTagValue_Invalid() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metricName\", \"tags\":{\"foo\":\"\"}, \"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		try
		{
			parser.parse();
			fail("Should throw ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("metric[0].tag[0].value may not be empty."));
		}
	}

	@Test
	public void test_tagValue_invalidCharacters() throws DatastoreException, IOException
	{
		String json = "[{\"name\": \"metricName\", \"tags\":{\"foo\":\"bad:value\"}, \"datapoints\": [[1,2]]}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		try
		{
			parser.parse();
			fail("Should throw ValidationException");
		}
		catch (ValidationException e)
		{
			assertThat(e.getMessage(), equalTo("metric[0].tag[0].value may only contain alphanumeric characters plus periods '.', slash '/', dash '-', and underscore '_'."));
		}
	}

	@Test
	public void test_validJsonWithTimestampValue() throws DatastoreException, IOException, ValidationException
	{
		String json = "[{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": 4321, \"tags\":{\"foo\":\"bar\"}}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		parser.parse();

		List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
		assertThat(dataPointSetList.size(), equalTo(1));

		assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
		assertThat(dataPointSetList.get(0).getTags().size(), equalTo(1));
		assertThat(dataPointSetList.get(0).getTags().get("foo"), equalTo("bar"));
		assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(1));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1234L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(4321L));
	}

	@Test(expected = MalformedJsonException.class)
	public void test_invaidJson() throws DatastoreException, IOException, ValidationException
	{
		String json = "[{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": }]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		parser.parse();

		List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
		assertThat(dataPointSetList.size(), equalTo(1));

		assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
		assertThat(dataPointSetList.get(0).getTags().size(), equalTo(0));
		assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(1));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1234L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(4321L));
	}

	@Test
	public void test_validJsonWithTimestampValueAndDataPoints() throws DatastoreException, IOException, ValidationException
	{
		String json = "[{\"name\": \"metric1\", \"timestamp\": 1234, \"value\": 4321, \"datapoints\": [[456, 654]], \"tags\":{\"foo\":\"bar\"}}]";

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		parser.parse();

		List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
		assertThat(dataPointSetList.size(), equalTo(1));

		assertThat(dataPointSetList.get(0).getName(), equalTo("metric1"));
		assertThat(dataPointSetList.get(0).getTags().size(), equalTo(1));
		assertThat(dataPointSetList.get(0).getTags().get("foo"), equalTo("bar"));
		assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(2));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(456L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(654L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(1).getTimestamp(), equalTo(1234L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(1).getLongValue(), equalTo(4321L));
	}

	@Test
	public void test_validJsonWithDatapoints() throws DatastoreException, IOException, ValidationException
	{
		String json = Resources.toString(Resources.getResource("json-metric-parser-multiple-metric.json"), Charsets.UTF_8);

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		parser.parse();

		List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
		assertThat(dataPointSetList.size(), equalTo(2));

		assertThat(dataPointSetList.get(0).getName(), equalTo("archive_file_tracked"));
		assertThat(dataPointSetList.get(0).getTags().size(), equalTo(1));
		assertThat(dataPointSetList.get(0).getTags().get("host"), equalTo("server1"));
		assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(3));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1349109376L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(123L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(1).getTimestamp(), equalTo(1349109377L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(1).getDoubleValue(), equalTo(13.2));
		assertThat(dataPointSetList.get(0).getDataPoints().get(2).getTimestamp(), equalTo(1349109378L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(2).getDoubleValue(), equalTo(23.1));

		assertThat(dataPointSetList.get(1).getName(), equalTo("archive_file_search"));
		assertThat(dataPointSetList.get(1).getTags().size(), equalTo(2));
		assertThat(dataPointSetList.get(1).getTags().get("host"), equalTo("server2"));
		assertThat(dataPointSetList.get(1).getTags().get("customer"), equalTo("Acme"));
		assertThat(dataPointSetList.get(1).getDataPoints().size(), equalTo(1));
		assertThat(dataPointSetList.get(1).getDataPoints().get(0).getTimestamp(), equalTo(1349109378L));
		assertThat(dataPointSetList.get(1).getDataPoints().get(0).getLongValue(), equalTo(321L));
	}
        
        @Test
	public void test_validJsonWithDatapointsAndMeta() throws DatastoreException, IOException, ValidationException
	{
		String json = Resources.toString(Resources.getResource("json-metric-parser-multiple-metric-with-meta.json"), Charsets.UTF_8);

		FakeDataStore fakeds = new FakeDataStore();
		KairosDatastore datastore = new KairosDatastore(fakeds, new QueryQueuingManager(1, "hostname"),
				Collections.<DataPointListener>emptyList(), "hostname");
		JsonMetricParser parser = new JsonMetricParser(datastore, new ByteArrayInputStream(json.getBytes()));

		parser.parse();

		List<DataPointSet> dataPointSetList = fakeds.getDataPointSetList();
		assertThat(dataPointSetList.size(), equalTo(2));

		assertThat(dataPointSetList.get(0).getName(), equalTo("archive_file_tracked"));
		assertThat(dataPointSetList.get(0).getTags().size(), equalTo(1));
		assertThat(dataPointSetList.get(0).getTags().get("host"), equalTo("server1"));
		assertThat(dataPointSetList.get(0).getDataPoints().size(), equalTo(3));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getTimestamp(), equalTo(1349109376L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(0).getLongValue(), equalTo(123L));
                assertThat(dataPointSetList.get(0).getDataPoints().get(0).getMetaValue(), equalTo(0L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(1).getTimestamp(), equalTo(1349109377L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(1).getDoubleValue(), equalTo(13.2));
                assertThat(dataPointSetList.get(0).getDataPoints().get(1).getMetaValue(), equalTo(1L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(2).getTimestamp(), equalTo(1349109378L));
		assertThat(dataPointSetList.get(0).getDataPoints().get(2).getDoubleValue(), equalTo(23.1));
                assertThat(dataPointSetList.get(0).getDataPoints().get(2).getMetaValue(), equalTo(2L));

		assertThat(dataPointSetList.get(1).getName(), equalTo("archive_file_search"));
		assertThat(dataPointSetList.get(1).getTags().size(), equalTo(2));
		assertThat(dataPointSetList.get(1).getTags().get("host"), equalTo("server2"));
		assertThat(dataPointSetList.get(1).getTags().get("customer"), equalTo("Acme"));
		assertThat(dataPointSetList.get(1).getDataPoints().size(), equalTo(1));
		assertThat(dataPointSetList.get(1).getDataPoints().get(0).getTimestamp(), equalTo(1349109378L));
		assertThat(dataPointSetList.get(1).getDataPoints().get(0).getLongValue(), equalTo(321L));
                assertThat(dataPointSetList.get(1).getDataPoints().get(0).getMetaValue(), equalTo(3L));
	}

	private static class FakeDataStore implements Datastore
	{
		List<DataPointSet> dataPointSetList = new ArrayList<DataPointSet>();

		protected FakeDataStore() throws DatastoreException
		{
		}

		public List<DataPointSet> getDataPointSetList()
		{
			return dataPointSetList;
		}

		@Override
		public void close() throws InterruptedException, DatastoreException
		{
		}

		@Override
		public void putDataPoints(DataPointSet dps) throws DatastoreException
		{
			dataPointSetList.add(dps);
		}

		@Override
		public Iterable<String> getMetricNames() throws DatastoreException
		{
			return null;
		}

		@Override
		public Iterable<String> getTagNames() throws DatastoreException
		{
			return null;
		}

		@Override
		public Iterable<String> getTagValues() throws DatastoreException
		{
			return null;
		}

		@Override
		public List<DataPointRow> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult) throws DatastoreException
		{
			return null;
		}

		@Override
		public void deleteDataPoints(DatastoreMetricQuery deleteQuery, CachedSearchResult cachedSearchResult) throws DatastoreException
		{
		}

		@Override
		public TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException
		{
			return null;  //To change body of implemented methods use File | Settings | File Templates.
		}
	}
}