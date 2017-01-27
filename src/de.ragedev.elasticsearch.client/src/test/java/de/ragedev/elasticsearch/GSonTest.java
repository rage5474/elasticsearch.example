package de.ragedev.elasticsearch;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.gson.Gson;

public class GSonTest {
	@Test
	public void simpleProtocolTest() throws Exception {
		REvent rEvent = new REvent(1, 1000, "MyEvent", "cpu.system");
		String json = new Gson().toJson(rEvent);
		REvent fromJson = new Gson().fromJson(json, REvent.class);

		assertEquals(rEvent.toString(), fromJson.toString());
	}
}
