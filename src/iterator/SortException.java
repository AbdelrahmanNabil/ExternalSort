package iterator;

import chainexception.ChainException;

public class SortException extends ChainException
{

	public SortException()
	{
		super();
	}

	public SortException(ChainException e, String s)
	{
		super(e, s);
	}
}
