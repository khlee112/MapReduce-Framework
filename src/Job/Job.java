package Job;

import java.util.List;

import Core.FileBlockList;
import Participant.ParticipantInfo;

/**
 * 
 * This class defines a map reduce job, implementing the following functions
 * ensures that the job will be properly distributed by the master.
 * 
 * */
public abstract class Job {

	public int jobID;

	public Job(int jobID) {
		this.jobID = jobID;
	}

	/** The number of file blocks in this job, equals to the number of maps. */
	abstract public int blockCount();

	/** Returns the file blocks. */
	abstract public FileBlockList getFileBlocks();

	/** Specifies how the blocks are to be processed. */
	abstract public MapperFunction getMapperFunction();

	/** Specifies how the mapper results are reduced. */
	abstract public ReducerFunction getReducerFunction();

	/** Specifies how the keys are assigned to reducers. */
	abstract public KeyToReducerFunction getKeyToReducerFunction(
			List<ParticipantInfo> reducers);
}
