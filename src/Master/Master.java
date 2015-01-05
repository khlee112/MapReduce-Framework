package Master;

import java.util.LinkedList;
import java.util.List;

import Job.Job;
import Job.KeyToReducerFunction;
import Job.ReducerFunction;
import Messages.Message;
import Messages.MessageBeginReduce;
import Messages.MessageMapperFinished;
import Messages.MessageReduceComplete;
import Messages.MessageType;
import Participant.ParticipantInfo;

/** Represents the coordinator, the final reducer. */
public class Master {

	private List<ParticipantStub> mapperStubs;
	private List<ParticipantStub> reducerStubs;
	private List<ParticipantStub> allParticipantStubs;

	protected Job job;
	private String outputFilename;
	private boolean isSorted;

	private RunnableCheckAlive checkAlive;
	private RunnableScheduler scheduler;

	// Records whether is block has been mapped, if all true, we can begin
	// reduce.
	private boolean[] isBlockProcessed;

	// Needs notifications from this many reducers for the job to finish.
	private int reducersAliveCount;
	private int reducerCompleteCount = 0;

	// True to print status automatically, false to use commands to monitor
	// status
	private boolean printOutput = true;

	// For display
	private boolean isReducePhase = false;
	private boolean isComplete = false;

	// For display
	private List<ParticipantStub> deadReducers;

	public Master(List<ParticipantStub> mapperStubs,
			List<ParticipantStub> reducerStubs, Job job, String outputFilename,
			boolean isSorted, boolean printOutput) {
		this.mapperStubs = mapperStubs;
		this.reducerStubs = reducerStubs;
		allParticipantStubs = new LinkedList<ParticipantStub>();
		allParticipantStubs.addAll(mapperStubs);
		allParticipantStubs.addAll(reducerStubs);

		this.job = job;
		this.outputFilename = outputFilename;
		this.isSorted = isSorted;
		this.printOutput = printOutput;

		for (ParticipantStub participantStub : allParticipantStubs) {
			participantStub.master = this;
			participantStub.connectToParticipant();
		}

		deadReducers = new LinkedList<ParticipantStub>();
	}

	public void start() {
		isReducePhase = false;

		System.out.println("Starting map phase");
		System.out.println("----------------------------------------");

		checkAlive = new RunnableCheckAlive(this);
		scheduler = new RunnableScheduler(this);
		// Default value of boolean is false
		isBlockProcessed = new boolean[job.blockCount()];

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}

		// Start a new thread for checking alive
		Thread checkAliveThread = new Thread(checkAlive);
		checkAliveThread.start();

		// Start a new thread for scheduling maps
		Thread schedulingThread = new Thread(scheduler);
		schedulingThread.start();
	}

	/** Called by scheduler if a reducer died */
	public void restart(int deadReducerID) {

		if (printOutput) {
			System.out.println("\nRestarting job due to reducer "
					+ deadReducerID + " connection lost.\n");
		}

		// Remove the participant info
		for (int i = 0; i < allParticipantStubs.size(); i++) {
			ParticipantStub current = allParticipantStubs.get(i);
			if (current.getID() == deadReducerID) {
				deadReducers.add(current);
				allParticipantStubs.remove(current);
			}
		}
		for (int i = 0; i < reducerStubs.size(); i++) {
			ParticipantStub current = reducerStubs.get(i);
			if (current.getID() == deadReducerID) {
				reducerStubs.remove(current);
			}
		}

		// Tell each participant to reset
		Message reset = new Message(MessageType.RESET_JOB, -1, job.jobID);
		for (ParticipantStub stub : allParticipantStubs) {
			sendMessageToParticipant(stub.getParticipantInfo(), reset);
		}

		// Restart
		checkAlive.stopSilent();
		scheduler.stopSilent();
		start();
	}

	public synchronized void receiveMessage(Message message) {
		int senderID = message.senderID;

		if (message.type == MessageType.CONFIRM_ALIVE) {

			// On confirm alive received, notify check alive runner
			checkAlive.onConfirmaAliveReceived(senderID);
		} else if (message.type == MessageType.MAPPER_FINISHED) {

			// A mapper has finished its job notify scheduler
			MessageMapperFinished finished = (MessageMapperFinished) message;
			int blockNumber = finished.blockNumber;
			int mapperID = senderID;

			if (printOutput) {
				System.out.printf("Mapper %d finished processing block %d\n",
						mapperID, blockNumber);
			}

			scheduler.onMapperFinished(finished);

			// Record that the block is processed
			isBlockProcessed[finished.blockNumber] = true;

			// Check if map phase is done
			if (canBeginReduce()) {
				beginReduce();
			}
		} else if (message.type == MessageType.REDUCE_FINISHED) {

			// A reducer has finished
			MessageReduceComplete complete = (MessageReduceComplete) message;
			reducerCompleteCount++;

			if (printOutput) {
				System.out
						.printf("Reducer %d finished reducing, results stored in: %s\n",
								complete.senderID, complete.outputFilename);
			}

			// Stop if all reduced
			if (reducerCompleteCount == reducersAliveCount) {
				isComplete = true;
				stop("Job Complete");
			}
		} else {
			System.out.println("Unrecognized message type: "
					+ message.type.toString());
		}
	}

	public void killAll() {
		checkAlive.stop();
		scheduler.stop();

		Message stop = new Message(MessageType.STOP, -1, -1);
		for (ParticipantStub stub : allParticipantStubs) {
			sendMessageToParticipant(stub.getParticipantInfo(), stop);
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
			}
		}

		System.out.println("Stopping...");
		System.exit(1);
	}

	private void beginReduce() {
		isReducePhase = true;
		reducersAliveCount = scheduler.getNumberOfReducersAlive();

		System.out.println("");
		System.out.printf("Starting reduce phase, %d reducers running\n",
				reducersAliveCount);
		System.out.println("----------------------------------------");

		// Tell each reducer to start reduce
		ReducerFunction reducerFunction = job.getReducerFunction();
		for (ParticipantStub reducer : reducerStubs) {
			Message beginReduce = new MessageBeginReduce(reducerFunction,
					outputFilename + "_reducer" + reducer.getID(), isSorted,
					job.jobID);
			sendMessageToParticipant(reducer.getParticipantInfo(), beginReduce);
		}
	}

	/** May be called by scheduler if no mappers or reducers are available. */
	public void stop(String message) {
		System.out.println("");
		System.out.println(message);
		checkAlive.stop();
		scheduler.stop();
		System.exit(1);
	}

	/** Called by check alive runnable */
	public void onParticipantDied(int participantID) {
		scheduler.onParticipantDied(participantID);
	}

	public synchronized void sendMessageToParticipant(ParticipantInfo info,
			Message message) {
		ParticipantStub participantStub = getParticipantStub(info.id);
		participantStub.sendToParticipant(message);
	}

	// Can begin reduce is all maps complete
	private boolean canBeginReduce() {
		for (int i = 0; i < isBlockProcessed.length; i++) {
			if (!isBlockProcessed[i]) {
				return false;
			}
		}
		return true;
	}

	/************************************
	 * GETTERS
	 ************************************/

	public List<ParticipantStub> getAllParticipants() {
		return allParticipantStubs;
	}

	public List<ParticipantStub> getMappers() {
		return mapperStubs;
	}

	public List<ParticipantStub> getReducers() {
		return reducerStubs;
	}

	public boolean[] getIsMapperAlive() {
		return scheduler.getIsMapperAlive();
	}

	public List<ParticipantStub> getDeadReducers() {
		return deadReducers;
	}

	public Job getJob() {
		return job;
	}

	public int getJobID() {
		return job.jobID;
	}

	public boolean getPrintOutput() {
		return printOutput;
	}

	public KeyToReducerFunction getKeyToReudcerFunction() {
		List<ParticipantInfo> reducerInfo = new LinkedList<ParticipantInfo>();
		for (ParticipantStub reducer : reducerStubs) {
			reducerInfo.add(reducer.getParticipantInfo());
		}
		return job.getKeyToReducerFunction(reducerInfo);
	}

	public ParticipantStub getParticipantStub(int participantID) {
		for (ParticipantStub participantStub : allParticipantStubs) {
			if (participantStub.getID() == participantID) {
				return participantStub;
			}
		}
		return null;
	}

	public String getCurrentPhase() {
		if (isComplete) {
			return "Complete";
		}

		return isReducePhase ? "Reduce Phase" : "Map Phase";
	}

	public int[] getBlockAssignments() {
		return scheduler.getBlockAssignments();
	}

	public boolean[] getIsBlockProcessed() {
		return isBlockProcessed;
	}
}
