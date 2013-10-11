package btree;

import chainexception.ChainException;


public class InsertException extends ChainException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public InsertException()
	{
	}

	public InsertException(String paramString)
	{
		super(null, paramString); 
	}

	public InsertException(Exception paramException, String paramString) 
	{ 
		super(paramException, paramString);
	}
}
