package Participant;

import java.io.Serializable;

/** Contains information regarding a particular participant */
public class ParticipantInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	public String host;
	public int port;
	public int cpuCount;
	public int id;

	public ParticipantInfo(String host, int port, int cpuCount, int id) {
		this.host = host;
		this.port = port;
		this.cpuCount = cpuCount;
		this.id = id;
	}

}
