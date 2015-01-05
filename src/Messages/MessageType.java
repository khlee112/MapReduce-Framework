package Messages;

/** Defines purpose of a message. */
public enum MessageType {
	// Master to participant for checking alive
	CHECK_ALIVE,

	// Participant to master for confirming alive
	CONFIRM_ALIVE,

	// Master to connect to participant
	CONNECTION_INIT,

	// Master to mapper for assigning a block to process
	ASSIGN_BLOCK,

	// Mapper to reducer for sending mapper output
	RECEIVE_MAPPER_OUTPUT,

	// Reducer to mapper saying that the result has been received
	MAPPER_OUTPUT_RECEIVED,

	// Mapper to master reporting that a block has been processed
	MAPPER_FINISHED,

	// Master to reducer for beginning to reduce
	BEGIN_REDUCE,

	// Reducer to master reporting that reduce has finished
	REDUCE_FINISHED,

	// Master to participant to specify that a job is restarting, clear previous
	// data
	RESET_JOB,

	// Master to participant to kill them
	STOP
}
