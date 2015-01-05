package WordCountJobExample;

import java.util.LinkedList;
import java.util.List;

import Job.KeyToReducerFunction;
import Participant.ParticipantInfo;

/** Returns a reducer info given a key */
public class WordCountKeyToReducerFunction extends KeyToReducerFunction {

	private static final long serialVersionUID = 1L;

	public WordCountKeyToReducerFunction(List<ParticipantInfo> reducers) {
		super(reducers);
	}

	@Override
	public ParticipantInfo getReducer(String key) {
		// Create a list of A-Z 0-9, the ranges are divided evenly between all
		// reducers.
		int reducerCount = reducerInfo.size();
		char firstChar = key.toUpperCase().charAt(0);

		// Add alphabets
		LinkedList<Character> characters = new LinkedList<Character>();
		for (int i = 0; i < 26; i++) {
			characters.add((char) ('A' + i));
		}
		// Add numbers
		for (int i = 0; i <= 9; i++) {
			characters.add((char) ('0' + i));
		}

		// Find index of first character
		int index = -1;
		for (int i = 0; i < characters.size(); i++) {
			if (characters.get(i).equals(firstChar)) {
				index = i;
				break;
			}
		}

		// If is not alpha numeric, just assign to first reducer
		if (index == -1) {
			return reducerInfo.get(0);
		}

		// Otherwise find corresponding reducer
		double percentile = index / (double) characters.size();
		int reducerIndex = (int) Math.floor(percentile * reducerCount);

		return reducerInfo.get(reducerIndex);
	}

}
