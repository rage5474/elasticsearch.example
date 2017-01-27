package de.ragedev.elasticsearch;

import lombok.Value;

@Value
public final class REvent {

	private long id;
	private long timestamp;
	private String value;
	private String channel;
}
