package iterator;

import chainexception.ChainException;

public class UnknowAttrType extends ChainException
{

	public UnknowAttrType()
	{
		super();
	}

	public UnknowAttrType(ChainException e, String s)
	{
		super(e, s);
	}
}
