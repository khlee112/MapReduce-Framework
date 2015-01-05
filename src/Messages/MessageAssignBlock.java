package Messages;

import Core.FileBlock;
import Job.KeyToReducerFunction;
import Job.MapperFunction;

/**
 * Represents a message from master to a mapper for assigning a block to
 * process.
 */
public class MessageAssignBlock extends Message {

	private static final long serialVersionUID = 1L;

	public FileBlock block;
	public MapperFunction mapperFunction;
	public int blockNumber;
	public KeyToReducerFunction keyToReducerFunction;

	public MessageAssignBlock(FileBlock block, MapperFunction mapperFunction,
			int blockNumber, KeyToReducerFunction keyToReducerFunction,
			int jobID) {

		// Sender is master
		super(MessageType.ASSIGN_BLOCK, -1, jobID);

		this.block = block;
		this.mapperFunction = mapperFunction;
		this.blockNumber = blockNumber;
		this.keyToReducerFunction = keyToReducerFunction;
	}
}
