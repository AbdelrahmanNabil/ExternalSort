package iterator;

import chainexception.ChainException;

public class UnknownKeyTypeException extends ChainException
{

	public UnknownKeyTypeException()
	{
		super();
	}
	
	public UnknownKeyTypeException(ChainException e, String s)
	{
		super(e, s);
	}
}
