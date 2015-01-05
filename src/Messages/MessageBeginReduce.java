package Messages;

import Job.ReducerFunction;

/** Message from master to reduce telling it to begin reducing. */
public class MessageBeginReduce extends Message {

	private static final long serialVersionUID = 1L;

	public ReducerFunction reducerFunction;
	public String outputFilename;
	public boolean isSorted;

	public MessageBeginReduce(ReducerFunction reducerFunction,
			String outputFilename, boolean isSorted, int jobID) {
		super(MessageType.BEGIN_REDUCE, -1, jobID);
		this.reducerFunction = reducerFunction;
		this.outputFilename = outputFilename;
		this.isSorted = isSorted;
	}

}
