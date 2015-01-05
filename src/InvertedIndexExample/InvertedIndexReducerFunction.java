package InvertedIndexExample;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import Job.MapperOutput;
import Job.ReducerFunction;

public class InvertedIndexReducerFunction extends ReducerFunction {

	private static final long serialVersionUID = 1L;
	private int delay;

	public InvertedIndexReducerFunction(int delay) {
		this.delay = delay;
	}

	@Override
	public void reduce(List<MapperOutput> results, String outputFileName,
			boolean sorted) {

		HashMap<String, List<Integer>> invertedIndex = new HashMap<String, List<Integer>>();

		// For each block
		for (MapperOutput result : results) {
			InvertedIndexMapperOutput invertedIndexBlockResult = (InvertedIndexMapperOutput) result;

			List<String> words = invertedIndexBlockResult.getWords();
			List<Integer> indices = invertedIndexBlockResult.getIndices();

			// For each word in the block, add to index
			for (int i = 0; i < words.size(); i++) {
				String word = words.get(i);
				int index = indices.get(i);
				if (invertedIndex.containsKey(word)) {
					List<Integer> list = invertedIndex.get(word);
					list.add(index);
					invertedIndex.put(word, list);
				} else {
					List<Integer> list = new LinkedList<Integer>();
					list.add(index);
					invertedIndex.put(word, list);
				}
			}
		}

		// Sort if specified
		List<String> words = new ArrayList<String>();
		for (String word : invertedIndex.keySet()) {
			words.add(word);
		}
		if (sorted) {
			Collections.sort(words);
		}

		// Output to file
		try {
			PrintWriter writer = new PrintWriter(outputFileName, "UTF-8");

			for (int i = 0; i < words.size(); i++) {
				String word = words.get(i);
				writer.print(word + ": ");
				List<Integer> list = invertedIndex.get(word);
				Collections.sort(list);
				for (Integer index : list)
					writer.print(index + ", ");
				writer.println();

				// Delay for UI
				try {
					Thread.sleep(delay);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			writer.close();
		} catch (Exception e) {
			System.out.println("Error: " + e.toString());
		}
	}

}
