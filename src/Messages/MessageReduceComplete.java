package Messages;

/** Message from reducer to master notifying that reduce has finished. */
public class MessageReduceComplete extends Message {

	private static final long serialVersionUID = 1L;

	public String outputFilename;

	public MessageReduceComplete(int reducerID, String outputFilename, int jobID) {
		super(MessageType.REDUCE_FINISHED, reducerID, jobID);
		this.outputFilename = outputFilename;
	}
}
