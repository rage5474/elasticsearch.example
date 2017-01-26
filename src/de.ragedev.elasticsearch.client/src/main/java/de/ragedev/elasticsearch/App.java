package de.ragedev.elasticsearch;

import java.io.IOException;
import java.util.Collections;

import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * Hello world!
 *
 */
public class App {

	public static void main(String[] args) {
		RestClient restClient = RestClient
				.builder(new HttpHost("localhost", 9200, "http"), new HttpHost("localhost", 9201, "http")).build();

		Response response;
		try {
			response = restClient.performRequest("DELETE", "_all", Collections.singletonMap("pretty", "true"));
			System.out.println("DELETE RESPONSE: " + EntityUtils.toString(response.getEntity()));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			Response indexResponse = restClient.performRequest("PUT", "/twitter/tweet/1",
					Collections.singletonMap("pretty", "true"),
					new NStringEntity(new Gson().toJson(new REvent("1", "X")), ContentType.APPLICATION_JSON));
			System.out.println("INSERT RESPONSE: " + EntityUtils.toString(indexResponse.getEntity()));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			Response indexResponse = restClient.performRequest("PUT", "/twitter/tweet/2",
					Collections.singletonMap("pretty", "true"),
					new NStringEntity(new Gson().toJson(new REvent("2", "Y")), ContentType.APPLICATION_JSON));
			System.out.println("INSERT RESPONSE: " + EntityUtils.toString(indexResponse.getEntity()));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			response = restClient.performRequest("POST", "/twitter/_refresh",
					Collections.singletonMap("pretty", "true"));
			System.out.println("REFRESH RESPONSE: " + EntityUtils.toString(response.getEntity()));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {
			response = restClient.performRequest("GET", "/twitter/tweet/_count",
					Collections.singletonMap("pretty", "true"));
			System.out.println("COUNT RESPONSE: " + EntityUtils.toString(response.getEntity()));

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			response = restClient.performRequest("GET", "/twitter/tweet/_search",
					Collections.singletonMap("pretty", "true"));
			System.out.println("SEARCH RESPONSE: " + EntityUtils.toString(response.getEntity()));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {
			response = restClient.performRequest("GET", "/twitter/tweet/1", Collections.singletonMap("pretty", "true"));
			String string = EntityUtils.toString(response.getEntity());
			System.out.println("GET RESPONSE: " + string);
			JsonObject jobj = new Gson().fromJson(string, JsonObject.class);
			
			JsonObject job = jobj.get("_source").getAsJsonObject();
			REvent rEvent = new Gson().fromJson(job, REvent.class);
			System.out.println(rEvent);
			

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			restClient.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
