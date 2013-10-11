package btree;

import heap.InvalidSlotNumberException;

import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

/**
 * Base class for a index file scan
 */
public abstract class IndexFileScan
{

	/**
	 * Get the next record.
	 * 
	 * @return the KeyDataEntry, which contains the key and data
	 * @throws InvalidFrameNumberException
	 * @throws HashEntryNotFoundException
	 * @throws PageUnpinnedException
	 * @throws ReplacerException
	 * @throws IOException
	 * @throws BufMgrException
	 * @throws PagePinnedException
	 * @throws BufferPoolExceededException
	 * @throws PageNotReadException
	 * @throws HashOperationException
	 * @throws InvalidSlotNumberException
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws KeyNotMatchException
	 */
	abstract public KeyDataEntry get_next()
			throws ReplacerException,
			PageUnpinnedException,
			HashEntryNotFoundException,
			InvalidFrameNumberException,
			IOException,
			HashOperationException,
			PageNotReadException,
			BufferPoolExceededException,
			PagePinnedException,
			BufMgrException,
			KeyNotMatchException,
			NodeNotMatchException,
			ConvertException,
			InvalidSlotNumberException;

	/**
	 * Delete the current record.
	 * 
	 * @throws IOException
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws KeyNotMatchException
	 * @throws InvalidSlotNumberException
	 * @throws DeleteRecException
	 */
	abstract public void delete_current()
			throws DeleteRecException,
			InvalidSlotNumberException,
			KeyNotMatchException,
			NodeNotMatchException,
			ConvertException,
			IOException;

	/**
	 * Returns the size of the key
	 * 
	 * @return the keysize
	 */
	abstract public int keysize();
}
