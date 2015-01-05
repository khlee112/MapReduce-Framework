package Participant;

import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import Job.MapperOutput;
import Job.ReducerFunction;
import Messages.Message;
import Messages.MessageBeginReduce;
import Messages.MessageMapperOutputReceived;
import Messages.MessageReceiveMapperOutput;
import Messages.MessageReduceComplete;
import Messages.MessageType;

/** Represents a reducer participant */

public class Reducer extends Participant {

	private Hashtable<Integer, List<MapperOutput>> jobIDToMapperOutput;

	public Reducer(ParticipantInfo participantInfo) {
		super(participantInfo);
		this.jobIDToMapperOutput = new Hashtable<Integer, List<MapperOutput>>();
	}

	public synchronized void receiveMessage(Message message) {
		int jobID = message.jobID;

		if (message.type == MessageType.CHECK_ALIVE) {

			// Master checking if alive, return confirmation
			sendConfirmAlive(jobID);
		} else if (message.type == MessageType.RECEIVE_MAPPER_OUTPUT) {

			// Received result from mapper, store the result
			MessageReceiveMapperOutput resultsMessage = (MessageReceiveMapperOutput) message;

			System.out.println("Reducer " + getID() + " received block "
					+ resultsMessage.blockNumber + " output from Mapper "
					+ resultsMessage.senderID);

			if (!jobIDToMapperOutput.containsKey(jobID)) {
				jobIDToMapperOutput.put(jobID, new LinkedList<MapperOutput>());
			}
			List<MapperOutput> mapperOutputs = jobIDToMapperOutput.get(jobID);
			mapperOutputs.add(resultsMessage.result);

			// Tell mapper that output has been received
			MessageMapperOutputReceived received = new MessageMapperOutputReceived(
					resultsMessage.blockNumber, getID(), jobID);
			sendMessageToParticipant(received, resultsMessage.mapperInfo, jobID);
		} else if (message.type == MessageType.BEGIN_REDUCE) {

			// Master requesting reducers to begin reduce
			MessageBeginReduce reduceMessage = (MessageBeginReduce) message;
			reduce(reduceMessage);
		} else if (message.type == MessageType.RESET_JOB) {

			// Clear intermediate data
			System.out.println("Reducer " + getID() + ": Resetting job "
					+ jobID);
			jobIDToMapperOutput.remove(jobID);
		} else if (message.type == MessageType.STOP) {

			// Stop any threads
			this.stop();

		} else {
			System.out.println("Unrecognized message type: "
					+ message.type.toString());
		}
	}

	/** Runs reduce in a separate thread and notify master when finished */
	private void reduce(final MessageBeginReduce message) {

		final List<MapperOutput> mapperOutputs = jobIDToMapperOutput
				.get(message.jobID);
		System.out.printf(
				"Reducer %d: starting reduce %d partial results for job %d\n",
				getID(), mapperOutputs.size(), message.jobID);

		new Thread(new Runnable() {
			public void run() {
				ReducerFunction reducerFunction = message.reducerFunction;
				String outputFilename = message.outputFilename;

				reducerFunction.reduce(mapperOutputs, outputFilename,
						message.isSorted);

				System.out.printf("Reducer %d: results written to: %s\n",
						getID(), outputFilename);

				// Tell master that reduce has finished
				MessageReduceComplete completeMessage = new MessageReduceComplete(
						getID(), outputFilename, message.jobID);
				sendMessageToMaster(completeMessage, completeMessage.jobID);

				// Clear mapper results
				jobIDToMapperOutput.remove(message.jobID);
			}
		}).start();
	}

}
