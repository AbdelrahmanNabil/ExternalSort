package btree;

import java.io.*;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

import global.*;
import heap.InvalidSlotNumberException;

/**
 * Base class for a index file
 */
public abstract class IndexFile
{

	/**
	 * Insert entry into the index file.
	 * 
	 * @param data
	 *            the key for the entry
	 * @param rid
	 *            the rid of the tuple with the key
	 * 
	 * @throws IOException
	 * @throws ConstructPageException
	 * @throws InsertException
	 * @throws BufMgrException
	 * @throws PagePinnedException
	 * @throws BufferPoolExceededException
	 * @throws PageNotReadException
	 * @throws InvalidFrameNumberException
	 * @throws PageUnpinnedException
	 * @throws HashOperationException
	 * @throws ReplacerException
	 * @throws HashEntryNotFoundException
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws KeyNotMatchException
	 * @throws InvalidSlotNumberException
	 * @throws InsertRecException
	 * @throws DeleteRecException
	 */
	abstract public void insert(final KeyClass data, final RID rid)
			throws IOException,
			ConstructPageException,
			InsertException,
			ReplacerException,
			HashOperationException,
			PageUnpinnedException,
			InvalidFrameNumberException,
			PageNotReadException,
			BufferPoolExceededException,
			PagePinnedException,
			BufMgrException,
			HashEntryNotFoundException,
			InvalidSlotNumberException,
			KeyNotMatchException,
			NodeNotMatchException,
			ConvertException,
			InsertRecException,
			DeleteRecException;

	/**
	 * Delete entry from the index file.
	 * 
	 * @param data
	 *            the key for the entry
	 * @param rid
	 *            the rid of the tuple with the key
	 * 
	 * @throws IOException
	 * @throws KeyNotMatchException
	 * @throws ConstructPageException
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws InvalidSlotNumberException
	 * @throws DeleteRecException
	 * @throws InvalidFrameNumberException
	 * @throws HashEntryNotFoundException
	 * @throws PageUnpinnedException
	 * @throws ReplacerException
	 */
	abstract public boolean Delete(final KeyClass data, final RID rid)
			throws InvalidSlotNumberException,
			NodeNotMatchException,
			ConvertException,
			ConstructPageException,
			KeyNotMatchException,
			IOException,
			DeleteRecException,
			ReplacerException,
			PageUnpinnedException,
			HashEntryNotFoundException,
			InvalidFrameNumberException;
}
