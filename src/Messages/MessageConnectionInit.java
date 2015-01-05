package Messages;

/**
 * 
 * Represent a message of initializing connection
 * 
 */

public class MessageConnectionInit extends Message {

	private static final long serialVersionUID = 1L;

	public MessageConnectionInit(int senderID, int jobID) {
		// Sender is master
		super(MessageType.CONNECTION_INIT, senderID, jobID);
	}

}
