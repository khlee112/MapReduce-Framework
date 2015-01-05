package Job;

import java.io.Serializable;
import java.util.List;

/** Implement this functor class to define how mapper results are reduced. */
abstract public class ReducerFunction implements Serializable {

	private static final long serialVersionUID = 1L;

	/** Function invoked on the reducer given an array of mapper outputs. */
	abstract public void reduce(List<MapperOutput> results,
			String outputFileName, boolean sorted);
}
