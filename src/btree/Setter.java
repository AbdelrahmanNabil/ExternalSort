package btree;

import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.Convert;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.Tuple;

public class Setter
{

	/**
	 * this method first pin the header page using the headerPage id and then
	 * convert the key Type,Key Length and root id to byte array and then write
	 * them at the first second third slot in the header page
	 * 
	 * @param pageid
	 * @param file
	 * @throws IOException
	 * @throws BufMgrException
	 * @throws PagePinnedException
	 * @throws BufferPoolExceededException
	 * @throws PageNotReadException
	 * @throws InvalidFrameNumberException
	 * @throws PageUnpinnedException
	 * @throws HashOperationException
	 * @throws ReplacerException
	 */
	public static void writeAttr(PageId pageid, BTreeFile file)
			throws ReplacerException,
			HashOperationException,
			PageUnpinnedException,
			InvalidFrameNumberException,
			PageNotReadException,
			BufferPoolExceededException,
			PagePinnedException,
			BufMgrException,
			IOException
	{

		// create a hfpage object
		HFPage page = new HFPage();

		// pin the header page
		SystemDefs.JavabaseBM.pinPage(pageid, page, false);

		page.init(pageid, page);

		page.setType(NodeType.BTHEAD);
		
		
		// create a byte array of length 4 bytes = 32/8
		byte[] tuple1 = new byte[4];

		// convert the keyType to byte array
		Convert.setIntValue(file.keyType, 0, tuple1);

		// insert the byte array into the header page
		page.insertRecord(tuple1);

		byte[] tuple2 = new byte[4];
		Convert.setIntValue(file.keyLength, 0, tuple2);
		page.insertRecord(tuple2);

		byte[] tuple3 = new byte[4];
		Convert.setIntValue(file.root.pid, 0, tuple3);
		page.insertRecord(tuple3);
	}

	/**
	 * this method first pin the header page and then read the first second and
	 * third records and get the byte arrays corresponging to each one and then
	 * convert them to integers and assign the variables of the BTreeFile to
	 * them
	 * 
	 * @param pageId
	 * @param file
	 * @throws IOException
	 * @throws BufMgrException
	 * @throws PagePinnedException
	 * @throws BufferPoolExceededException
	 * @throws PageNotReadException
	 * @throws InvalidFrameNumberException
	 * @throws PageUnpinnedException
	 * @throws HashOperationException
	 * @throws ReplacerException
	 * @throws InvalidSlotNumberException
	 */
	public static void readAttr(PageId pageId, BTreeFile file)
			throws ReplacerException,
			HashOperationException,
			PageUnpinnedException,
			InvalidFrameNumberException,
			PageNotReadException,
			BufferPoolExceededException,
			PagePinnedException,
			BufMgrException,
			IOException,
			InvalidSlotNumberException
	{

		HFPage page = new HFPage();

		// pin the header page
		SystemDefs.JavabaseBM.pinPage(pageId, page, false);

		// get the first record in the page
		RID rid = new RID(pageId, 0);

		// get the tuple from it
		Tuple tuple = page.getRecord(rid);

		// convert it to integer and assign the keyType to it
		file.keyType = Convert.getIntValue(0, tuple.getTupleByteArray());

		// get the second record in the page
		rid = new RID(pageId, 1);
		tuple = page.getRecord(rid);
		file.keyLength = Convert.getIntValue(0, tuple.getTupleByteArray());

		// get the third record in the page
		rid = new RID(pageId, 2);
		tuple = page.getRecord(rid);
		int pid = Convert.getIntValue(0, tuple.getTupleByteArray());
		file.root = new PageId(pid);
	}
}
