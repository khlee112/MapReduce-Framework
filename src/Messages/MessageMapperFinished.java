package Messages;

/**
 * Represents a message from mapper to master specifying that a block has been
 * processed.
 */
public class MessageMapperFinished extends Message {

	private static final long serialVersionUID = 1L;

	public int blockNumber;

	public MessageMapperFinished(int blockNumber, int senderID, int jobID) {
		super(MessageType.MAPPER_FINISHED, senderID, jobID);
		this.blockNumber = blockNumber;
	}

}
