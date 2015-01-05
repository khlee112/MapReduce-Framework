package Job;

import java.io.Serializable;

/** Stores partial results after a file block is processed by a mapper. */
public abstract class MapperOutput implements Serializable {

	private static final long serialVersionUID = 1L;

	public abstract String getKey();

}
