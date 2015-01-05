package WordCountJobExample;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import Job.MapperOutput;
import Job.ReducerFunction;

/** Tally the word counts from the mappers, outputs to a file. */
public class WordCountReducerFunction extends ReducerFunction {

	private static final long serialVersionUID = 1L;
	private int delay;

	public WordCountReducerFunction(int delay) {
		this.delay = delay;
	}

	public void reduce(List<MapperOutput> results, String outputFileName,
			boolean sorted) {

		HashMap<String, Integer> wordCount = new HashMap<String, Integer>();

		// For each mapper output
		for (MapperOutput result : results) {
			WordCountMapperOutput mapperOutput = (WordCountMapperOutput) result;
			List<String> words = mapperOutput.getWords();

			// For each word in the block, increment its count
			for (String word : words) {
				if (wordCount.containsKey(word)) {
					int count = wordCount.get(word) + 1;
					wordCount.put(word, count);
				} else {
					wordCount.put(word, 1);
				}
			}
		}

		// Sort if specified
		List<String> words = new ArrayList<String>();
		for (String word : wordCount.keySet()) {
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
				writer.println(word + " " + wordCount.get(word));

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
