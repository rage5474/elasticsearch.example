package de.ragedev.elasticsearch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;

import org.elasticsearch.client.transport.TransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ElasticSearchSimpleWorkflowTest {

	private static final String MESSAGES_INDEX = "messages";
	private TransportClient databaseConnection;

	@Before
	public void setup() {
		databaseConnection = ElasticSearchDBHelper.openConnectionToLocalDB();

		if (ElasticSearchDBHelper.indexExists(databaseConnection, MESSAGES_INDEX)) {
			ElasticSearchDBHelper.deleteIndex(databaseConnection, MESSAGES_INDEX);
		}
	}

	@Test
	public void indexNotExisting() throws Exception {
		assertFalse(ElasticSearchDBHelper.indexExists(databaseConnection, MESSAGES_INDEX));
	}

	@Test
	public void addEventToDB() throws Exception {

		REvent event1 = new REvent(1, 1000, "Message" + 1, "cpu.system");
		REvent event2 = new REvent(2, 2000, "Message" + 2, "cpu.system");
		REvent event3 = new REvent(3, 3000, "Message" + 3, "cpu.system");

		ElasticSearchDBHelper.addEventToIndex(databaseConnection, MESSAGES_INDEX, event1);
		ElasticSearchDBHelper.addEventToIndex(databaseConnection, MESSAGES_INDEX, event2);
		ElasticSearchDBHelper.addEventToIndex(databaseConnection, MESSAGES_INDEX, event3);

		REvent dbEvent1 = ElasticSearchDBHelper.getEvent(databaseConnection, MESSAGES_INDEX, "cpu.system", 1);
		REvent dbEvent2 = ElasticSearchDBHelper.getEvent(databaseConnection, MESSAGES_INDEX, "cpu.system", 2);
		REvent dbEvent3 = ElasticSearchDBHelper.getEvent(databaseConnection, MESSAGES_INDEX, "cpu.system", 3);

		assertEquals(3, ElasticSearchDBHelper.numberOfEvents(databaseConnection));
		assertEquals(event1, dbEvent1);
		assertEquals(event2, dbEvent2);
		assertEquals(event3, dbEvent3);

	}

	@Test
	public void addEventsToDB() throws Exception {

		REvent event4 = new REvent(4, 4000, "Message" + 4, "cpu.system");
		REvent event5 = new REvent(5, 5000, "Message" + 5, "cpu.system");
		REvent event6 = new REvent(6, 6000, "Message" + 6, "cpu.system");

		ElasticSearchDBHelper.addEventsToIndex(databaseConnection, MESSAGES_INDEX,
				Arrays.asList(event4, event5, event6));

		REvent dbEvent4 = ElasticSearchDBHelper.getEvent(databaseConnection, MESSAGES_INDEX, "cpu.system", 4);
		REvent dbEvent5 = ElasticSearchDBHelper.getEvent(databaseConnection, MESSAGES_INDEX, "cpu.system", 5);
		REvent dbEvent6 = ElasticSearchDBHelper.getEvent(databaseConnection, MESSAGES_INDEX, "cpu.system", 6);

		assertEquals(3, ElasticSearchDBHelper.numberOfEvents(databaseConnection));
		assertEquals(event4, dbEvent4);
		assertEquals(event5, dbEvent5);
		assertEquals(event6, dbEvent6);
	}

	@After
	public void cleanup() {
		ElasticSearchDBHelper.closeLocalDB(databaseConnection);
	}

}
