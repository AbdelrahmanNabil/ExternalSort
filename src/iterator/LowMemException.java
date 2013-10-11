package iterator;

import chainexception.ChainException;

public class LowMemException extends ChainException
{

	public LowMemException()
	{
		super();
	}

	public LowMemException(ChainException e, String s)
	{
		super(e, s);
	}
}
