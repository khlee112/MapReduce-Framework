package Core;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 
 * This class divides input file into blocks, write the serialized FileBlock
 * into files, and load files when FileBlock being accessed.
 * 
 */

public class FileBlockList {

	HashMap<Integer, Long> idToFilePointer;
	private String filename;
	private transient RandomAccessFile file;
	private int linesPerBlock;

	public FileBlockList(String filename, int linesPerBlock) {
		idToFilePointer = new HashMap<Integer, Long>();
		this.linesPerBlock = linesPerBlock;
		this.filename = filename;
	}

	public void add(int blockNumber, long filePointer) {
		idToFilePointer.put(blockNumber, filePointer);
	}

	public FileBlock get(int blockIndex) {
		long filePointer = idToFilePointer.get(blockIndex);
		return readFileBlock(blockIndex, filePointer);
	}

	public int size() {
		return idToFilePointer.size();
	}

	public void closeFile() throws IOException {
		file.close();
	}

	public FileBlock readFileBlock(int blockIndex, long filePointer) {

		FileBlock fileBlock = null;
		int currentLineCount = blockIndex * linesPerBlock;
		int startingLineCount = currentLineCount;

		try {
			// Initialization
			RandomAccessFile fileIS = new RandomAccessFile(filename, "rws");
			fileIS.seek(filePointer);

			List<String> currentBlockLines = new ArrayList<String>();
			String line = fileIS.readLine();

			// For each line
			while (line != null) {
				currentBlockLines.add(line);
				currentLineCount++;

				// If a block is filled up, create a new file block
				if (currentLineCount % linesPerBlock == 0) {
					fileBlock = new FileBlock(currentBlockLines, blockIndex,
							startingLineCount);
					break;
				}
				line = fileIS.readLine();
			}

			// Create block, if it does not contain as many lines.
			if (currentBlockLines.size() > 0) {
				fileBlock = new FileBlock(currentBlockLines, blockIndex,
						startingLineCount);
			}
			fileIS.close();
		} catch (Exception e) {
			System.out.println("Error: " + e.toString());
		}
		return fileBlock;
	}

}
