package WordCountJobExample;

import java.util.LinkedList;
import java.util.List;

import Job.MapperOutput;

/** Stores a list of word occurrences */
public class WordCountMapperOutput extends MapperOutput {

	private static final long serialVersionUID = 1L;

	private List<String> words;

	public WordCountMapperOutput() {
		words = new LinkedList<String>();
	}

	public void addWord(String word) {
		words.add(word);
	}

	public List<String> getWords() {
		return words;
	}

	@Override
	public String getKey() {
		// The list contains the same strings, so the first word can be the key
		return words.get(0);
	}
}
