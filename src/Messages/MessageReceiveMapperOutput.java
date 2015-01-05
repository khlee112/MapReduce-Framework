package Messages;

import Job.MapperOutput;
import Participant.ParticipantInfo;

/**
 * Represents a message from mapper to reducer containing the output for a file
 * block;
 */
public class MessageReceiveMapperOutput extends Message {

	private static final long serialVersionUID = 1L;

	public MapperOutput result;
	public int blockNumber;

	// Used by reducer for ack
	public ParticipantInfo mapperInfo;

	public MessageReceiveMapperOutput(MapperOutput result, int blockNumber,
			int mapperID, int jobID, ParticipantInfo mapperInfo) {

		super(MessageType.RECEIVE_MAPPER_OUTPUT, mapperID, jobID);
		this.result = result;
		this.blockNumber = blockNumber;
		this.mapperInfo = mapperInfo;
	}

}
