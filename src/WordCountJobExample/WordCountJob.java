package WordCountJobExample;

import java.util.List;

import Core.FileBlockList;
import Core.Utils;
import Job.Job;
import Job.KeyToReducerFunction;
import Job.MapperFunction;
import Job.ReducerFunction;
import Participant.ParticipantInfo;

/** Example job for counting the occurrences of words */
public class WordCountJob extends Job {

	private FileBlockList fileBlocks;
	private int delay;

	public WordCountJob(int jobID, String inputFilename, int linesPerBlock,
			int delay) {
		super(jobID);
		fileBlocks = Utils.breakIntoBlocks(inputFilename, linesPerBlock);
		this.delay = delay;
	}

	public int blockCount() {
		return fileBlocks.size();
	}

	public FileBlockList getFileBlocks() {
		return fileBlocks;
	}

	public MapperFunction getMapperFunction() {
		return new WordCountMapperFunction(delay);
	}

	public ReducerFunction getReducerFunction() {
		return new WordCountReducerFunction(delay);
	}

	public KeyToReducerFunction getKeyToReducerFunction(
			List<ParticipantInfo> reducers) {
		return new WordCountKeyToReducerFunction(reducers);
	}
}
