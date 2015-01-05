package Core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import Job.Job;
import Master.Master;
import Master.ParticipantStub;

public class UserInteraction {

	public static void beginUserInteraction(Master master) {
		InputStreamReader isReader = new InputStreamReader(System.in);
		BufferedReader bufferRead = new BufferedReader(isReader);
		String line = "";

		// Just an infinite loop that reads lines and parses them
		while (line != null && !line.equalsIgnoreCase("kill")) {
			try {
				line = bufferRead.readLine();

				if (line != null) {
					runCommand(line, master);
				}
			} catch (IOException e) {
				System.out.println(e.toString());
			}
		}

	}

	private static void runCommand(String line, Master master) {
		if (line.equalsIgnoreCase("status")) {
			printStatus(master);
		}
		if (line.equalsIgnoreCase("blocks")) {
			printFileBlocks(master);
		}
		if (line.equalsIgnoreCase("kill")) {
			master.killAll();
//			master.stop("Killing participants...");
		}
	}

	private static void printStatus(Master master) {

		Job job = master.getJob();

		System.out.println("\n");
		System.out.println("---------------------");
		System.out.println("Status:");
		System.out.println("---------------------");

		System.out.println("Job ID: " + job.jobID);
		System.out.println("Current phase: " + master.getCurrentPhase());

		List<ParticipantStub> mappers = master.getMappers();
		List<ParticipantStub> reducers = master.getReducers();
		boolean[] isMapperAlive = master.getIsMapperAlive();
		List<ParticipantStub> deadReducers = master.getDeadReducers();

		for (int i = 0; i < mappers.size(); i++) {
			ParticipantStub mapper = mappers.get(i);
			System.out.println("Mapper " + mapper.getID() + ": "
					+ (isMapperAlive[i] ? "Healthy" : "Unreachable"));
		}
		for (int i = 0; i < reducers.size(); i++) {
			ParticipantStub reducer = reducers.get(i);
			System.out.println("Reducer " + reducer.getID() + ": Healthy");
		}
		for (int i = 0; i < deadReducers.size(); i++) {
			ParticipantStub deadReducer = deadReducers.get(i);
			System.out.println("Reducer " + deadReducer.getID()
					+ ": Unreachable");
		}
		System.out.println("");
	}

	private static void printFileBlocks(Master master) {
		int[] blockAssignments = master.getBlockAssignments();
		boolean[] isBlockProcessed = master.getIsBlockProcessed();

		System.out.println("\n");
		System.out.println("---------------------");
		System.out.println("Block Status:");
		System.out.println("---------------------");

		for (int i = 0; i < blockAssignments.length; i++) {
			boolean assigned = blockAssignments[i] != -1;

			if (assigned) {
				boolean isFinished = isBlockProcessed[i];

				if (isFinished) {
					System.out.println("Block " + i + ":  processed by mapper "
							+ blockAssignments[i]);
				} else {
					System.out.println("Block " + i
							+ ":  processing on mapper " + blockAssignments[i]);
				}

			} else {
				System.out.println("Block " + i + ": Unassigned");
			}
		}
	}
}
