package InvertedIndexExample;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import Core.FileBlock;
import Job.KeyToReducerFunction;
import Job.MapperFunction;
import Job.MapperOutput;

/** Outputs a map from word to occurrences */
public class InvertedIndexMapperFunction extends MapperFunction {

	private static final long serialVersionUID = 1L;
	private int delay;

	public InvertedIndexMapperFunction(int delay) {
		this.delay = delay;
	}

	public List<MapperOutput> map(FileBlock fileBlock,
			KeyToReducerFunction keyToReducer) {

		HashMap<Integer, InvertedIndexMapperOutput> keyToOutputs = new HashMap<Integer, InvertedIndexMapperOutput>();
		int totalLines = fileBlock.getLineCount();
		int currentLineCount = fileBlock.getStartingLineIndex();

		// Tokenize each line and store
		for (int i = 0; i < totalLines; i++) {
			String line = fileBlock.getLine(i);
			String[] tokens = line.split(" ");

			for (String token : tokens) {

				// Find the output with the same key
				if (!keyToOutputs
						.containsKey(keyToReducer.getReducer(token).id)) {
					keyToOutputs.put(keyToReducer.getReducer(token).id,
							new InvertedIndexMapperOutput());
				}
				InvertedIndexMapperOutput output = keyToOutputs
						.get(keyToReducer.getReducer(token).id);

				// Add the word to the output
				output.addWord(token, currentLineCount + i);
			}
		}

		// Format the map into a list
		List<MapperOutput> outputList = new LinkedList<MapperOutput>();
		for (Integer id : keyToOutputs.keySet()) {
			outputList.add(keyToOutputs.get(id));

			// Delay for UI
			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return outputList;
	}

}
