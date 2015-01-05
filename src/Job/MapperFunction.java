package Job;

import java.io.Serializable;
import java.util.List;

import Core.FileBlock;

/** Implement this functor class to define how a block is processed in a mapper. */
abstract public class MapperFunction implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * This method will be called in a mapper to process a block, output is a
	 * list of MapperOutputs containing the same key.
	 */
	abstract public List<MapperOutput> map(FileBlock fileBlock, KeyToReducerFunction keyToReducer);
}
