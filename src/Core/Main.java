package Core;

import java.util.ArrayList;
import java.util.List;

import InvertedIndexExample.InvertedIndexJob;
import Job.Job;
import Master.Master;
import Master.ParticipantStub;
import Participant.Mapper;
import Participant.ParticipantInfo;
import Participant.Reducer;
import WordCountJobExample.WordCountJob;

public class Main {

	private static final String CONFIG_FILE = "Config.txt";
	private static final String WORD_COUNT_INPUT_FILE = "SampleInput.txt";
	private static final int LINES_PER_BLOCK = 50;
	private static final boolean SORTED = true;

	public static void main(String[] args) {
		if (args[0].equalsIgnoreCase("wordCount")
				|| args[0].equalsIgnoreCase("invertedIndex")) {

			String jobName = args[0];
			String outputFileName = args[1];
			int jobID = Integer.parseInt(args[2]);
			int delay = Integer.parseInt(args[3]);
			boolean autoPrint = args.length == 5;

			if (jobName.equalsIgnoreCase("wordCount")) {
				runJob(jobName, jobID, CONFIG_FILE, delay, autoPrint,
						outputFileName);
			} else {
				runJob(jobName, jobID, CONFIG_FILE, delay, autoPrint,
						outputFileName);
			}

		} else if (args.length == 5) {
			// Host name, port, cpuCount, ID
			ParticipantInfo participantInfo = new ParticipantInfo(args[0],
					Integer.parseInt(args[1]), Integer.parseInt(args[2]),
					Integer.parseInt(args[3]));
			if (args[4].equalsIgnoreCase("Mapper")) {
				System.out.println("Starting Mapper " + participantInfo.id);
				new Mapper(participantInfo);
			} else if (args[4].equalsIgnoreCase("Reducer")) {
				System.out.println("Starting Reducer " + participantInfo.id);
				new Reducer(participantInfo);
			} else {
				System.out.println("Wrong participant type");
			}
		} else {
			System.out
					.println("Master Usage: <job name> <outputFileName> <job ID> <delay time> [auto]\n"
							+ "Slave Usage: <hostname> <port> <cpu count> <nodeID> <Mapper/Reducer>");
		}
	}

	private static void runJob(String jobName, int jobID, String config,
			int delay, boolean autoPrint, String outputFilename) {
		// Load participants
		List<ParticipantStub> mapperStubs = new ArrayList<ParticipantStub>();
		List<ParticipantStub> reducerStubs = new ArrayList<ParticipantStub>();
		Utils.getMappersAndReducers(config, mapperStubs, reducerStubs);
		displayParticipants(mapperStubs, reducerStubs);

		// Create job
		Job job = null;
		if (jobName.equalsIgnoreCase("wordCount")) {
			job = new WordCountJob(jobID, WORD_COUNT_INPUT_FILE,
					LINES_PER_BLOCK, delay);
		} else {
			job = new InvertedIndexJob(jobID, WORD_COUNT_INPUT_FILE,
					LINES_PER_BLOCK, delay);
		}

		// Create and start master to handle scheduling
		Master master = new Master(mapperStubs, reducerStubs, job,
				outputFilename, SORTED, autoPrint);
		master.start();

		if (!autoPrint) {
			UserInteraction.beginUserInteraction(master);
		}
	}

	private static void displayParticipants(List<ParticipantStub> mapperStubs,
			List<ParticipantStub> reducerStubs) {
		for (ParticipantStub mapperStub : mapperStubs) {
			System.out.printf("%s %d: host=%s port=%d #cpu=%d\n", "Mapper ID",
					mapperStub.getID(), mapperStub.getHost(),
					mapperStub.getPort(), mapperStub.getCPUCount());
		}
		for (ParticipantStub reducerStub : reducerStubs) {
			System.out.printf("%s %d: host=%s port=%d #cpu=%d\n", "Reducer ID",
					reducerStub.getID(), reducerStub.getHost(),
					reducerStub.getPort(), reducerStub.getCPUCount());
		}
		System.out.println("");
	}
}
