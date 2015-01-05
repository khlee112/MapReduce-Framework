package Master;

import java.util.List;

import Core.FileBlock;
import Core.FileBlockList;
import Job.Job;
import Job.KeyToReducerFunction;
import Job.MapperFunction;
import Messages.MessageAssignBlock;
import Messages.MessageMapperFinished;
import Participant.ParticipantInfo;

/** Runnable used by master to dispatch work to participants. */
public class RunnableScheduler implements Runnable {

	private static final int LOOP_DELAY = 100;
	private static final int UNASSIGNED = -1;

	private Master master;
	private List<ParticipantStub> mappers;
	private List<ParticipantStub> reducers;

	private FileBlockList fileBlocks;
	private MapperFunction mapperFunction;
	private KeyToReducerFunction keyToReducerFunction;

	private boolean shouldStop = false;

	// Records the mapper ID the block is assigned to. -1 = unassigned.
	private int[] blockMapperID;

	// Indicates which participants are reachable
	private boolean[] isMapperAlive;
	private boolean[] isReducerAlive;

	// The number of blocks that each mapper can take, equal #cpu - #blocks
	// being processed.
	private int[] availableCapacities;

	public RunnableScheduler(Master master) {
		this.master = master;
		this.mappers = master.getMappers();
		this.reducers = master.getReducers();

		Job job = master.getJob();
		this.fileBlocks = job.getFileBlocks();
		this.mapperFunction = job.getMapperFunction();
		this.keyToReducerFunction = master.getKeyToReudcerFunction();

		// Initialize all blocks to unassigned
		blockMapperID = new int[fileBlocks.size()];
		for (int i = 0; i < fileBlocks.size(); i++) {
			blockMapperID[i] = UNASSIGNED;
		}

		// Initialize all participants as alive
		isMapperAlive = new boolean[mappers.size()];
		isReducerAlive = new boolean[reducers.size()];
		for (int i = 0; i < mappers.size(); i++) {
			isMapperAlive[i] = true;
		}
		for (int i = 0; i < reducers.size(); i++) {
			isReducerAlive[i] = true;
		}

		// Initialize available capacities to #cpu
		availableCapacities = new int[mappers.size()];
		for (int i = 0; i < mappers.size(); i++) {
			int cpuCount = mappers.get(i).getParticipantInfo().cpuCount;
			availableCapacities[i] = cpuCount;
		}
	}

	@Override
	public void run() {
		while (!shouldStop) {
			while (!shouldStop) {
				// Delay a little before assignments
				try {
					Thread.sleep(LOOP_DELAY);
				} catch (InterruptedException e) {
					System.out.println("Error: " + e.toString());
				}

				for (int blockIndex = 0; blockIndex < fileBlocks.size(); blockIndex++) {
					// Find an unassigned file block
					if (blockMapperID[blockIndex] == UNASSIGNED) {
						assignBlock(blockIndex);
					}
				}
			}
		}
//		System.out.println("Scheduling thread stopped.");
	}

	private void assignBlock(int blockIndex) {
		FileBlock currentBlock = fileBlocks.get(blockIndex);

		// Find a free mapper
		ParticipantInfo mapperInfo = null;
		for (int mapperIndex = 0; mapperIndex < mappers.size(); mapperIndex++) {
			if (isMapperAlive[mapperIndex]
					&& availableCapacities[mapperIndex] > 0) {
				availableCapacities[mapperIndex]--;
				mapperInfo = mappers.get(mapperIndex).getParticipantInfo();
				break;
			}
		}
		// Do nothing if no free mapper
		if (mapperInfo == null) {
			return;
		}

		// Set assignment info
		blockMapperID[blockIndex] = mapperInfo.id;

		// Send assign message
		MessageAssignBlock message = new MessageAssignBlock(currentBlock,
				mapperFunction, blockIndex, keyToReducerFunction,
				master.getJobID());
		master.sendMessageToParticipant(mapperInfo, message);

		if (master.getPrintOutput()) {
			System.out.println("Assigning block " + blockIndex + " to mapper "
					+ mapperInfo.id);
		}
	}

	public void stop() {
		shouldStop = true;
	}

	public void stopSilent() {
		shouldStop = true;
	}

	public synchronized void onParticipantDied(int participantID) {
		if (isAlive(participantID)) {
			if (isMapper(participantID)) {
				onMapperDied(participantID);
			} else {
				onReducerDied(participantID);
			}
		}

		// End if no mapper or reducer available
		boolean allMappersDead = true;
		boolean allReducersDead = true;

		for (int i = 0; i < isMapperAlive.length; i++) {
			if (isMapperAlive[i]) {
				allMappersDead = false;
			}
		}
		for (int i = 0; i < isReducerAlive.length; i++) {
			if (isReducerAlive[i]) {
				allReducersDead = false;
			}
		}
		if (allMappersDead) {
			master.stop("Stopping job due to no mappers reachable");
		} else if (allReducersDead) {
			master.stop("Stopping job due to no reducers reachable");
		}

		// If a reducer died and not all reducers dead, restart job
		if (!allReducersDead && !isMapper(participantID)) {
			master.restart(participantID);
		}
	}

	private void onMapperDied(int mapperID) {

		if (master.getPrintOutput()) {
			System.out.println("!!!-----Mapper " + mapperID
					+ " connection lost-----!!!");
		}

		// Set alive to false
		int mapperIndex = getIndex(mapperID);
		isMapperAlive[mapperIndex] = false;

		// Set block corresponding to mapper as unassigned
		for (int i = 0; i < fileBlocks.size(); i++) {
			if (blockMapperID[i] == mapperID) {
				blockMapperID[i] = UNASSIGNED;
			}
		}
	}

	private void onReducerDied(int reducerID) {

		if (master.getPrintOutput()) {
			System.out.println("!!!-----Reducer " + reducerID
					+ " connection lost-----!!!");
		}

		// Set alive to false
		int reducerIndex = getIndex(reducerID);
		isReducerAlive[reducerIndex] = false;
	}

	public synchronized void onMapperFinished(
			MessageMapperFinished finishedMessage) {
		// Increment the available capacity of the mapper
		int mapperIndex = getIndex(finishedMessage.senderID);
		availableCapacities[mapperIndex]++;
	}

	public int getNumberOfReducersAlive() {
		int count = 0;
		for (int i = 0; i < isReducerAlive.length; i++) {
			if (isReducerAlive[i]) {
				count++;
			}
		}
		return count;
	}

	/*********************************************
	 * Private helpers
	 *********************************************/

	// Returns whether the participant is a mapper or a reducer
	private boolean isMapper(int participantID) {
		for (ParticipantStub mapper : mappers) {
			if (mapper.getID() == participantID) {
				return true;
			}
		}
		return false;
	}

	// Returns whether the participant is alive
	private boolean isAlive(int participantID) {
		int index = getIndex(participantID);
		if (isMapper(participantID)) {
			return isMapperAlive[index];
		} else {
			return isReducerAlive[index];
		}
	}

	// Returns the index of the participant in either reducers or mappers list
	private int getIndex(int participantID) {
		for (int i = 0; i < mappers.size(); i++) {
			if (mappers.get(i).getID() == participantID) {
				return i;
			}
		}
		for (int i = 0; i < reducers.size(); i++) {
			if (reducers.get(i).getID() == participantID) {
				return i;
			}
		}
		System.out.printf("Exception: participant ID %d not found!\n",
				participantID);
		return -1;
	}

	/************************************
	 * GETTERS
	 ************************************/
	public boolean[] getIsMapperAlive() {
		return isMapperAlive;
	}

	public int[] getBlockAssignments() {
		return blockMapperID;
	}
}
