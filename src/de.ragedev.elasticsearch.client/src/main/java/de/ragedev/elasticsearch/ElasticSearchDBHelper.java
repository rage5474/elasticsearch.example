package de.ragedev.elasticsearch;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.google.gson.Gson;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class ElasticSearchDBHelper {

	private static final String MESSAGES_INDEX = "messages";

	private static final Gson gson = new Gson();

	@SuppressWarnings("resource")
	public static TransportClient openConnectionToLocalDB() {
		TransportClient client = null;
		try {
			client = new PreBuiltTransportClient(Settings.EMPTY)
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		} catch (UnknownHostException e) {
			log.error("Couldn't connect to local DB. Maybe it was not started...", e);
		}
		return client;
	}

	public static void addEventsToIndex(TransportClient client, String messagesIndex, List<REvent> events) {
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for (REvent nextEvent : events) {
			bulkRequest.add(client.prepareIndex(messagesIndex, nextEvent.getChannel(), "" + nextEvent.getId())
					.setSource(gson.toJson(nextEvent)));
		}

		BulkResponse bulkResponse = bulkRequest.get();
		if (bulkResponse.hasFailures()) {
			log.error("addEventsToIndex failed.");
		}
		log.trace(bulkResponse.toString());
	}

	public static REvent getEvent(TransportClient client, String indexName, String channel, long id) {
		GetResponse getResponse = client.prepareGet(indexName, channel, "" + id).get();
		return gson.fromJson(getResponse.getSourceAsString(), REvent.class);
	}

	public static void flushIndex(TransportClient client, String indexName) {
		client.admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();
	}

	public static void addEventToIndex(TransportClient client, String messagesIndex, REvent event) {
		IndexResponse response = client.prepareIndex(messagesIndex, event.getChannel(), "" + event.getId())
				.setSource(gson.toJson(event)).get();
		log.trace(response.toString());
	}

	public static void deleteIndex(TransportClient client, String indexName) {
		DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
		if (!delete.isAcknowledged()) {
			log.error("Index " + indexName + " wasn't deleted");
		} else {
			log.trace("Index " + MESSAGES_INDEX + " deleted.");
		}
	}

	public static boolean indexExists(TransportClient client, String messagesIndex) {
		boolean exists = client.admin().indices().prepareExists(messagesIndex).execute().actionGet().isExists();
		return exists;
	}

	public static long numberOfEvents(TransportClient client) {
		SearchResponse searchResponse = client.prepareSearch(MESSAGES_INDEX).setQuery(matchAllQuery()).setSize(0).get();
		log.trace(searchResponse.toString());
		return searchResponse.getHits().getTotalHits();
	}

	public static void closeLocalDB(TransportClient client) {
		if (client != null)
			client.close();
	}

}
