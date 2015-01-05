package InvertedIndexExample;

import java.util.LinkedList;
import java.util.List;

import Job.MapperOutput;

/** Stores a list of word indices */
public class InvertedIndexMapperOutput extends MapperOutput {

	private static final long serialVersionUID = 1L;

	private List<String> words;
	private List<Integer> indices;

	public InvertedIndexMapperOutput() {
		words = new LinkedList<String>();
		indices = new LinkedList<Integer>();
	}

	public void addWord(String word, int index) {
		words.add(word);
		indices.add(index);
	}

	public List<String> getWords() {
		return words;
	}

	public List<Integer> getIndices() {
		return indices;
	}

	@Override
	public String getKey() {
		// The list contains the same strings, so the first word can be the key
		return words.get(0);
	}
}
