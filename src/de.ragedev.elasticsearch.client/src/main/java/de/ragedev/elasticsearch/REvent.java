package de.ragedev.elasticsearch;

class REvent {
	private String name;
	private String source;

	public REvent(String name, String source) {
		this.name = name;
		this.source = source;
	}

	@Override
	public String toString() {
		return "REvent [name=" + name + ", source=" + source + "]";
	}
	
	
}
