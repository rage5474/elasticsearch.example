package de.ragedev.elasticsearch;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.gson.Gson;

public class GSonTest {
	@Test
	public void testName() throws Exception {
		REvent rEvent = new REvent("X","Y");
		String json = new Gson().toJson(rEvent);
		System.out.println(json);
		REvent fromJson = new Gson().fromJson(json, REvent.class);
		
		assertEquals(rEvent.toString(), fromJson.toString());
	}
}
