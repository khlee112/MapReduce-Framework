package Messages;

/**
 * Represents a message from reducer to mapper after receiving the output.
 */
public class MessageMapperOutputReceived extends Message {

	private static final long serialVersionUID = 1L;

	public int blockNumber;

	public MessageMapperOutputReceived(int blockNumber, int reducerID, int jobID) {

		super(MessageType.MAPPER_OUTPUT_RECEIVED, reducerID, jobID);
		this.blockNumber = blockNumber;
	}

}
