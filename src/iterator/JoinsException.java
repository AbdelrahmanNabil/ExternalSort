package iterator;

import chainexception.ChainException;

public class JoinsException extends ChainException
{

	public JoinsException()
	{
		super();
	}

	public JoinsException(ChainException e, String s)
	{
		super(e, s);
	}
}
