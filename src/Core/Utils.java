package Core;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.util.List;

import Master.ParticipantStub;

public class Utils {

	/** Populated the list of mappers and reducers from configuration */
	public static void getMappersAndReducers(String configFilename,
			List<ParticipantStub> mappers, List<ParticipantStub> reducers) {
		System.out.println("Parsing configuration");
		System.out.println("----------------------------------------");

		try {
			// Initialization
			FileReader fileReader = new FileReader(configFilename);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			// Line format: host port #CPU Mapper/Reducer
			String line = bufferedReader.readLine();
			int lineNumber = 1;

			// Parse each line
			while (line != null) {
				String[] tokens = line.split(" ");
				String host = tokens[0];
				int port = Integer.parseInt(tokens[1]);
				int cpuCount = Integer.parseInt(tokens[2]);

				// Type is either "Mapper" or "Reducer", ID is the line number
				String specifiedType = tokens[3];
				if (specifiedType.equals("Mapper")) {
					ParticipantStub mapper = new ParticipantStub(host, port,
							cpuCount, lineNumber);
					mappers.add(mapper);
				} else {
					ParticipantStub reducer = new ParticipantStub(host, port,
							cpuCount, lineNumber);
					reducers.add(reducer);
				}

				line = bufferedReader.readLine();
				lineNumber++;
			}
			bufferedReader.close();
		} catch (Exception e) {
			System.out.println("Error: " + e.toString());
		}
	}

	/**
	 * Breaks a file into blocks. This is called when the program is
	 * initialized, sort of like a pseudo replication, in which we assume that
	 * the blocks are already replicated on the participants.
	 */
	public static FileBlockList breakIntoBlocks(String filename,
			int linesPerBlock) {

		FileBlockList fileBlocks = new FileBlockList(filename, linesPerBlock);
		int currentBlockNumber = 0;
		int currentBlockLineNumber = 0;
		int currentLineCount = 0;
		long filePointer = 0;
		long prevFilePointer = 0;

		System.out.println("Reading file: " + filename);
		System.out.println("----------------------------------------");

		try {
			// Initialization
			RandomAccessFile fileIS = new RandomAccessFile(filename, "rws");

			filePointer = fileIS.getFilePointer();
			String line = fileIS.readLine();

			// For each line
			while (line != null) {
				currentBlockLineNumber++;
				currentLineCount++;

				// If a new block is filled up, create a new file block
				if (currentLineCount % linesPerBlock == 0) {
					fileBlocks.add(currentBlockNumber, prevFilePointer);
					prevFilePointer = filePointer;
					currentBlockNumber++;
					currentBlockLineNumber = 0;
				}

				filePointer = fileIS.getFilePointer();
				line = fileIS.readLine();
			}

			// Add the final block, may not contain as many lines.
			if (currentBlockLineNumber > 0) {
				fileBlocks.add(currentBlockNumber, prevFilePointer);
			}

			fileIS.close();
		} catch (Exception e) {
			System.out.println("Error: " + e.toString());
		}

		System.out.println("Lines in file: " + currentLineCount);
		System.out.println("Lines per block: " + linesPerBlock);
		System.out.println("Blocks created: " + currentBlockNumber);
		System.out.println("");

		return fileBlocks;
	}
}
