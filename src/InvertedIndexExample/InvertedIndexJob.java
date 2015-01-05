package InvertedIndexExample;

import java.util.List;

import Core.FileBlockList;
import Core.Utils;
import Job.Job;
import Job.KeyToReducerFunction;
import Job.MapperFunction;
import Job.ReducerFunction;
import Participant.ParticipantInfo;

/** Example job for create inverted index */
public class InvertedIndexJob extends Job {

	private FileBlockList fileBlocks;
	private int delay;

	public InvertedIndexJob(int jobID, String inputFilename, int linesPerBlock, int delay) {
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
		return new InvertedIndexMapperFunction(delay);
	}

	public ReducerFunction getReducerFunction() {
		return new InvertedIndexReducerFunction(delay);
	}

	public KeyToReducerFunction getKeyToReducerFunction(
			List<ParticipantInfo> reducers) {
		return new InvertedIndexKeyToReducerFunction(reducers);
	}

}
