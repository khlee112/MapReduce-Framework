package Job;

import java.io.Serializable;
import java.util.List;

import Participant.ParticipantInfo;

/** Implement this functor class to define how a key is assigned to a reducer. */
abstract public class KeyToReducerFunction implements Serializable {

	private static final long serialVersionUID = 1L;

	protected List<ParticipantInfo> reducerInfo;

	public KeyToReducerFunction(List<ParticipantInfo> reducers) {
		this.reducerInfo = reducers;
	}

	/**
	 * This method will be called in a mapper, returns reducer information given
	 * a key.
	 */
	abstract public ParticipantInfo getReducer(String key);
}
