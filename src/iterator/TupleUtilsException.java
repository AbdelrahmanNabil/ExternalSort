package iterator;

import chainexception.ChainException;

public class TupleUtilsException extends ChainException
{

	public TupleUtilsException()
	{
		super();
	}
	
	public TupleUtilsException(ChainException e, String s)
	{
		super(e, s);
	}
}
