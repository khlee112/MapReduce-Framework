package Messages;

import java.io.Serializable;

/**
 * Represents a message used for communication between the master and the
 * participants.
 */
public class Message implements Serializable {

	private static final long serialVersionUID = 1L;

	public MessageType type;

	public int senderID;
	public int jobID;

	public Message(MessageType type, int senderID, int jobID) {
		this.type = type;
		this.senderID = senderID;
		this.jobID = jobID;
	}

}
