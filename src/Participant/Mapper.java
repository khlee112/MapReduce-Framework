package Participant;

import java.util.Hashtable;
import java.util.List;

import Core.FileBlock;
import Job.MapperFunction;
import Job.MapperOutput;
import Messages.Message;
import Messages.MessageAssignBlock;
import Messages.MessageMapperFinished;
import Messages.MessageMapperOutputReceived;
import Messages.MessageReceiveMapperOutput;
import Messages.MessageType;

/** Represents a mapper participant */
public class Mapper extends Participant {

	// For each job, for each block, how many acks are still needed before we
	// can tell master that the block has been processed
	private Hashtable<Integer, Hashtable<Integer, Integer>> jobIDBlockNumberAcksRequired;

	public Mapper(ParticipantInfo participantInfo) {
		super(participantInfo);
		jobIDBlockNumberAcksRequired = new Hashtable<Integer, Hashtable<Integer, Integer>>();
	}

	public synchronized void receiveMessage(Message message) {
		int jobID = message.jobID;

		if (message.type == MessageType.CHECK_ALIVE) {

			// Master checking if alive, return confirmation
			sendConfirmAlive(jobID);
		} else if (message.type == MessageType.ASSIGN_BLOCK) {

			// Run processing in a new thread
			MessageAssignBlock assignMessage = (MessageAssignBlock) message;
			runProcessing(assignMessage);
		} else if (message.type == MessageType.MAPPER_OUTPUT_RECEIVED) {

			// Reducer received message, check if we can tell master the block
			// is done
			MessageMapperOutputReceived received = (MessageMapperOutputReceived) message;
			int blockNumber = received.blockNumber;
			int countsNeeded = jobIDBlockNumberAcksRequired.get(jobID).get(
					blockNumber);
			countsNeeded--;
			jobIDBlockNumberAcksRequired.get(jobID).put(received.blockNumber,
					countsNeeded);

			if (countsNeeded == 0) {
				// Notify master that a block is processed
				MessageMapperFinished finished = new MessageMapperFinished(
						blockNumber, getID(), message.jobID);
				sendMessageToMaster(finished, finished.jobID);

				System.out.println("Mapper " + getID()
						+ " finished mapping block " + blockNumber);
			}
		} else if (message.type == MessageType.RESET_JOB) {

			// Clear intermediate data
			System.out
					.println("Mapper " + getID() + ": Resetting job " + jobID);
			jobIDBlockNumberAcksRequired.remove(jobID);

		} else if (message.type == MessageType.STOP) {

			// Stop any threads
			this.stop();
		} else {
			System.out.println("Unrecognized message type: "
					+ message.type.toString());
		}
	}

	/** Runs processing in a separate thread and sends results to reducer */
	private void runProcessing(final MessageAssignBlock message) {

		System.out.println("Mapper " + getID() + " assigned block "
				+ message.blockNumber);

		new Thread(new Runnable() {
			public void run() {
				MapperFunction mapperFunction = message.mapperFunction;
				FileBlock fileBlock = message.block;

				// Get the list of outputs containing the same key
				List<MapperOutput> results = mapperFunction.map(fileBlock,
						message.keyToReducerFunction);

				// Store number of acks required from reducers
				int jobID = message.jobID;
				if (!jobIDBlockNumberAcksRequired.containsKey(jobID)) {
					jobIDBlockNumberAcksRequired.put(jobID,
							new Hashtable<Integer, Integer>());
				}
				jobIDBlockNumberAcksRequired.get(jobID).put(
						message.blockNumber, results.size());

				// Send results to reducers
				for (MapperOutput output : results) {

					// Get the key and decide with reducer to send to
					String key = output.getKey();
					ParticipantInfo reducerInfo = message.keyToReducerFunction
							.getReducer(key);

					// Send the partial output to the reducer
					Message messageToReducer = new MessageReceiveMapperOutput(
							output, message.blockNumber, getID(),
							message.jobID, participantInfo);
					sendMessageToParticipant(messageToReducer, reducerInfo,
							message.jobID);
				}
			}
		}).start();
	}
}
