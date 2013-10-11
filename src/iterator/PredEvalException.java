package iterator;

import chainexception.ChainException;

public class PredEvalException extends ChainException
{

	public PredEvalException()
	{
		super();
	}
	
	public PredEvalException(ChainException e, String s)
	{
		super(e, s);
	}
}
