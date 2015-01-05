package Core;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a subsection of a file;
 * */
public class FileBlock implements Serializable {

	private static final long serialVersionUID = 1L;

	private List<String> lines;
	private int blockNumber;
	private int startingLineIndex;

	public FileBlock(List<String> lines, int blockNumber, int startingLineIndex) {
		this.lines = lines;
		this.blockNumber = blockNumber;
		this.startingLineIndex = startingLineIndex;
	}

	public String getLine(int index) {
		return lines.get(index);
	}

	public int getBlockNumber() {
		return blockNumber;
	}

	public int getLineCount() {
		return lines.size();
	}

	public int getStartingLineIndex() {
		return startingLineIndex;
	}

}
