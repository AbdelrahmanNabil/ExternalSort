package iterator;

import chainexception.ChainException;

public class IndexException extends ChainException
{

	public IndexException()
	{
		super();
	}
	
	public IndexException(ChainException e, String s)
	{
		super(e, s);
	}
}
