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

package org.kairosdb.core.http.rest;

import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.MalformedJsonException;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.datastore.QueryResults;
import org.kairosdb.core.formatter.DataFormatter;
import org.kairosdb.core.formatter.FormatterException;
import org.kairosdb.core.formatter.JsonFormatter;
import org.kairosdb.core.formatter.JsonResponse;
import org.kairosdb.core.http.rest.json.ErrorResponse;
import org.kairosdb.core.http.rest.json.GsonParser;
import org.kairosdb.core.http.rest.json.JsonMetricParser;
import org.kairosdb.core.http.rest.json.JsonResponseBuilder;
import org.kairosdb.core.http.rest.validation.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.ws.rs.core.Response.ResponseBuilder;

enum NameType
{
	METRIC_NAMES,
	TAG_KEYS,
	TAG_VALUES
}

@Path("/api/v1")
public class MetricsResource
{
	private static final Logger log = LoggerFactory.getLogger(MetricsResource.class);

	private final KairosDatastore datastore;
	private final Map<String, DataFormatter> formatters = new HashMap<String, DataFormatter>();
	private final GsonParser gsonParser;

	@Inject
	public MetricsResource(KairosDatastore datastore, GsonParser gsonParser)
	{
		this.datastore = checkNotNull(datastore);
		this.gsonParser= checkNotNull(gsonParser);
		formatters.put("json", new JsonFormatter());
	}

	private void setHeaders(ResponseBuilder responseBuilder)
	{
		responseBuilder.header("Access-Control-Allow-Origin", "*");
		responseBuilder.header("Pragma", "no-cache");
		responseBuilder.header("Cache-Control", "no-cache");
		responseBuilder.header("Expires", 0);
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/version")
	public Response getVersion()
	{
		Package thisPackage = getClass().getPackage();
		String versionString = thisPackage.getImplementationTitle()+" "+thisPackage.getImplementationVersion();
		ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity("{\"version\": \""+versionString+"\"}");
		setHeaders(responseBuilder);
		return responseBuilder.build();
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/metricnames")
	public Response getMetricNames()
	{
		return executeNameQuery(NameType.METRIC_NAMES);
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/tagnames")
	public Response getTagNames()
	{
		return executeNameQuery(NameType.TAG_KEYS);
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/tagvalues")
	public Response getTagValues()
	{
		return executeNameQuery(NameType.TAG_VALUES);
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Consumes("application/gzip")
	@Path("/datapoints")
	public Response addGzip(InputStream gzip)
	{
		GZIPInputStream gzipInputStream;
		try
		{
			gzipInputStream = new GZIPInputStream(gzip);
		}
		catch (IOException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addError(e.getMessage()).build();
		}
		return (add(gzipInputStream));
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints")
	public Response add(InputStream json)
	{
		try
		{
			JsonMetricParser parser = new JsonMetricParser(datastore, json);
			parser.parse();

			return Response.status(Response.Status.NO_CONTENT).build();
		}
		catch (ValidationException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addError(e.getMessage()).build();
		}
		catch(MalformedJsonException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addError(e.getMessage()).build();
		}
		catch (Exception e)
		{
			log.error("Failed to add metric.", e);
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage())).build();
		}
	}


	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints/query")
	public Response get(String json) throws Exception
	{
		// todo verify that end time is not before start time.
		checkNotNull(json);

		try
		{
			File respFile = File.createTempFile("kairos", ".json");
			BufferedWriter writer = new BufferedWriter(new FileWriter(respFile));

			JsonResponse jsonResponse = new JsonResponse(writer);

			jsonResponse.begin();
			jsonResponse.startQueries();

			List<QueryMetric> queries = gsonParser.parseQueryMetric(json);

			for (QueryMetric query : queries)
			{
				QueryResults qr = datastore.query(query);

				try
				{
					jsonResponse.formatQuery(qr);
				}
				finally
				{
					if (qr != null)
						qr.close();
				}
			}

			jsonResponse.endQueries();

			jsonResponse.end();
			writer.flush();
			writer.close();

			ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(
					new FileStreamingOutput(respFile));

			setHeaders(responseBuilder);
			return responseBuilder.build();
		}
		catch (JsonSyntaxException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addError(e.getMessage()).build();
		}
		catch (QueryException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addError(e.getMessage()).build();
		}
		catch (BeanValidationException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addErrors(e.getErrorMessages()).build();
		}
		catch (Exception e)
		{
			log.error("Query failed.", e);
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage())).build();
		}
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints/delete")
	public Response delete(String json) throws Exception
	{
		checkNotNull(json);

		try
		{
			List<QueryMetric> queries = gsonParser.parseQueryMetric(json);

			for (QueryMetric query : queries)
			{
				datastore.delete(query);
			}

			ResponseBuilder responseBuilder = Response.status(Response.Status.NO_CONTENT);
			responseBuilder.header("Access-Control-Allow-Origin", "*");
			return responseBuilder.build();
		}
		catch (JsonSyntaxException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addError(e.getMessage()).build();
		}
		catch (QueryException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addError(e.getMessage()).build();
		}
		catch (BeanValidationException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addErrors(e.getErrorMessages()).build();
		}
		catch (Exception e)
		{
			log.error("Delete failed.", e);
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage())).build();
		}
	}

	/**
	 Information for this endpoint was taken from
	 https://developer.mozilla.org/en-US/docs/HTTP/Access_control_CORS

	 Response to a cors preflight request to access data.
	 */
	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints/query")
	public Response corsPreflightQuery(@HeaderParam("Access-Control-Request-Headers")String requestHeaders,
			@HeaderParam("Access-Control-Request-Method")String requestMethod)
	{
		ResponseBuilder responseBuilder = Response.status(Response.Status.OK);
		responseBuilder.header("Access-Control-Allow-Origin", "*");
		responseBuilder.header("Access-Control-Max-Age", "86400"); //Cache for one day
		responseBuilder.header("Access-Control-Allow-Headers", requestHeaders);
		if (requestMethod != null)
			responseBuilder.header("Access-Control-Allow_Method", requestMethod);
		return (responseBuilder.build());
	}

	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints")
	public Response corsPreflightDataPoints(@HeaderParam("Access-Control-Request-Headers")String requestHeaders,
			@HeaderParam("Access-Control-Request-Method")String requestMethod)
	{
		return (corsPreflightQuery(requestHeaders, requestMethod));
	}

	@DELETE
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/metric/{metricName}")
	public Response metricDelete(@PathParam("metricName") String metricName) throws Exception
	{
		try
		{
			QueryMetric query = new QueryMetric(0L, Long.MAX_VALUE, 0, metricName);
			datastore.delete(query);

			ResponseBuilder responseBuilder = Response.status(Response.Status.NO_CONTENT);
			responseBuilder.header("Access-Control-Allow-Origin", "*");
			return responseBuilder.build();
		}
		catch (Exception e)
		{
			log.error("Delete failed.", e);
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage())).build();
		}
	}

	private Response executeNameQuery(NameType type)
	{
		try
		{
			Iterable<String> values = null;
			switch(type)
			{
				case METRIC_NAMES:
					values = datastore.getMetricNames();
					break;
				case TAG_KEYS:
					values = datastore.getTagNames();
					break;
				case TAG_VALUES:
					values = datastore.getTagValues();
					break;
			}

			DataFormatter formatter = formatters.get("json");

			ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(
					new ValuesStreamingOutput(formatter, values));
			setHeaders(responseBuilder);
			return responseBuilder.build();
		}
		catch (Exception e)
		{
			log.error("Failed to get " + type, e);
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(
					new ErrorResponse(e.getMessage())).build();
		}
	}

	public class ValuesStreamingOutput implements StreamingOutput
	{
		private DataFormatter m_formatter;
		private Iterable<String> m_values;

		public ValuesStreamingOutput(DataFormatter formatter, Iterable<String> values)
		{
			m_formatter = formatter;
			m_values = values;
		}

		@SuppressWarnings("ResultOfMethodCallIgnored")
		public void write(OutputStream output) throws IOException, WebApplicationException
		{
			Writer writer = new OutputStreamWriter(output,  "UTF-8");

			try
			{
				m_formatter.format(writer, m_values);
			}
			catch (FormatterException e)
			{
				log.error("Description of what failed:", e);
			}

			writer.flush();
		}
	}

	public class FileStreamingOutput implements StreamingOutput
	{
		private File m_responseFile;

		public FileStreamingOutput(File responseFile)
		{
			m_responseFile = responseFile;
		}

		@Override
		public void write(OutputStream output) throws IOException, WebApplicationException
		{
			InputStream reader = new FileInputStream(m_responseFile);

			byte[] buffer = new byte[1024];
			int size;

			while ((size = reader.read(buffer)) != -1)
			{
				output.write(buffer, 0, size);
			}

			reader.close();
			output.flush();
			m_responseFile.delete();
		}
	}
}
