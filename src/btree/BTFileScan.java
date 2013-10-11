package btree;

import java.io.IOException;

import diskmgr.Page;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.InvalidSlotNumberException;

public class BTFileScan extends IndexFileScan
{

	public BTLeafPage	currentPage;
	public RID			currentRid;
	public KeyClass		hiKey;
	public boolean		flag;
	public int			keyLength;
	/**
	 * used to determine last scanned record
	 */
	private RID			prev;

	public BTFileScan(int rootpid, int keyType, KeyClass lowkey, KeyClass hiKey, int keyLength)
			throws ReplacerException,
			HashOperationException,
			PageUnpinnedException,
			InvalidFrameNumberException,
			PageNotReadException,
			BufferPoolExceededException,
			PagePinnedException,
			BufMgrException,
			IOException,
			InvalidSlotNumberException,
			NodeNotMatchException,
			ConvertException,
			ConstructPageException,
			KeyNotMatchException,
			DeleteRecException,
			HashEntryNotFoundException
	{

		this.keyLength = keyLength;

		flag = true;

		this.hiKey = hiKey;

		RID ke = scan(new PageId(rootpid), lowkey, keyType);

		this.currentRid = ke;

		if (currentRid != null)
		{
			Page pge = new Page();

			SystemDefs.JavabaseBM.pinPage(currentRid.pageNo, pge, false);

			currentPage = new BTLeafPage(pge, keyType);
		}
	}

	/**
	 * This method is used to get the starting pageId
	 * this method supports looking for values between the range
	 * between lo_key and hi_key included
	 * 
	 * (1) lo_key = null, hi_key = null scan the whole index
	 * (2) lo_key = null, hi_key!= null range scan from min to the hi_key
	 * (3) lo_key!= null, hi_key = null range scan from the lo_key to max
	 * (4) lo_key!= null, hi_key!= null, lo_key = hi_key exact match (might not
	 * unique)
	 * (5) lo_key!= null, hi_key!= null, lo_key < hi_key
	 * 
	 * @param troot
	 * @param lowkey
	 * @param keyType
	 * 
	 * @return
	 * 
	 * @throws IOException
	 * @throws InvalidSlotNumberException
	 * @throws NodeNotMatchException
	 * @throws ConvertException
	 * @throws ConstructPageException
	 * @throws KeyNotMatchException
	 * @throws DeleteRecException
	 * @throws InvalidFrameNumberException
	 * @throws HashEntryNotFoundException
	 * @throws PageUnpinnedException
	 * @throws ReplacerException
	 * @throws BufMgrException
	 * @throws PagePinnedException
	 * @throws BufferPoolExceededException
	 * @throws PageNotReadException
	 * @throws HashOperationException
	 */
	private RID scan(PageId troot, KeyClass lowkey, int keyType)
			throws IOException,
			InvalidSlotNumberException,
			NodeNotMatchException,
			ConvertException,
			ConstructPageException,
			KeyNotMatchException,
			DeleteRecException,
			ReplacerException,
			PageUnpinnedException,
			HashEntryNotFoundException,
			InvalidFrameNumberException,
			HashOperationException,
			PageNotReadException,
			BufferPoolExceededException,
			PagePinnedException,
			BufMgrException
	{

		/**
		 * @Aamer
		 *        this method does the same job as when it was in BTreeFIle
		 *        but transfered to BTFIleScan that's all with minor updates
		 *        to support all the keys cases which are listed above
		 *        in method documentation , this method will also supports
		 *        getting the smallest values between the range as a start
		 *        not just looking for the same value.
		 */
		BTSortedPage pge = new BTSortedPage(troot, keyType);

		// check if leaf page
		if (pge.getType() == NodeType.LEAF)
		{
			BTLeafPage lfpge = new BTLeafPage(pge, keyType);

			// max no of rcrds in page
			int max = lfpge.numberOfRecords();

			RID recid = new RID();

			// get first record
			KeyDataEntry entry = lfpge.getFirst(recid);

			PageId next = lfpge.getNextPage();

			while (entry == null && next.pid != -1)
			{
				SystemDefs.JavabaseBM.unpinPage(lfpge.getCurPage(), false);

				SystemDefs.JavabaseBM.pinPage(next, lfpge, false);

				max = lfpge.numberOfRecords();

				entry = lfpge.getFirst(recid);

				next = lfpge.getNextPage();
			}

			// all pages are empty nothing to scan
			if (entry == null)
			{
				SystemDefs.JavabaseBM.unpinPage(lfpge.getCurPage(), false);

				return null;
			}
			/**
			 * @Aamer
			 *        update here to just return the first record rid
			 *        when lowKey is null
			 */
			if (lowkey != null)
			{
				for (int i = 0; i < max; i++)
				{
					/**
					 * @Aamer
					 *        this method is updated to get any values
					 *        between lo_key and hi_key
					 */
					if (BT.keyCompare(entry.key, lowkey) >= 0)
					{
						if (hiKey != null)
						{
							if (BT.keyCompare(entry.key, hiKey) <= 0)
							{
								// unpin pinned pages before return
								SystemDefs.JavabaseBM.unpinPage(lfpge.getCurPage(), false);

								// found
								return recid;
							}
							else
							{
								// unpin pinned pages before return
								SystemDefs.JavabaseBM.unpinPage(lfpge.getCurPage(), false);

								// not found
								return null;
							}
						}
						else
						{
							// unpin pinned pages before return
							SystemDefs.JavabaseBM.unpinPage(lfpge.getCurPage(), false);

							// found
							return recid;
						}
					}

					entry = lfpge.getNext(recid);// try to search in the next
													// record
				}// for loop
			}
			else
			{
				// unpin pinned pages before return
				SystemDefs.JavabaseBM.unpinPage(lfpge.getCurPage(), false);

				return recid;
			}

			// unpin pinned pages before return
			SystemDefs.JavabaseBM.unpinPage(lfpge.getCurPage(), false);

			// not found
			return null;
		}

		// check if this page is BTIndexPage
		else if (pge.getType() == NodeType.INDEX)
		{
			BTIndexPage indpge = new BTIndexPage(pge, keyType);

			// iterate records rids
			RID rid_i = new RID();

			KeyDataEntry prev = new KeyDataEntry((KeyClass) null, indpge.getLeftLink());// left most

			KeyDataEntry entry = indpge.getFirst(rid_i);// right

			// max no of rcrds in page
			int max = indpge.numberOfRecords();

			/**
			 * @Aamer
			 *        update here to just go left when lowkey is null
			 */
			if (lowkey != null)
				// get the first key lager than given key
				// to choose the correct subtree
				for (int i = 0; i < max && BT.keyCompare(entry.key, lowkey) < 0; i++)
				{
					// set the prev pointer
					prev.key = entry.key;
					prev.data = entry.data;

					entry = indpge.getNext(rid_i);
				}

			// unpin pinned pages before return
			SystemDefs.JavabaseBM.unpinPage(indpge.getCurPage(), false);

			return scan(((IndexData) prev.data).getData(), lowkey, keyType);
		}
		return null;
	}

	/**
	 * Iterate once (during a scan).
	 * 
	 * Returns:
	 * 
	 * null if done; otherwise next KeyDataEntry
	 * 
	 * Overrides: get_next in class IndexFileScan
	 * 
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
	 * */
	@Override
	public KeyDataEntry get_next()
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
			InvalidSlotNumberException
	{

		if (flag)
		{
			/**
			 * no pages to scan
			 */
			if (currentPage == null)
				return null;

			// check if current record is null this means that it has reached
			// the
			// last of the page
			if (currentRid == null)
			{
				int keyType = currentPage.keyType;

				// get the page next to the current page
				PageId nextId = currentPage.getNextPage();

				// un pin the current page from the pool
				SystemDefs.JavabaseBM.unpinPage(currentPage.getCurPage(), true);

				currentPage = null;

				/**
				 * @Aamer
				 *        reached end of leafPages
				 */
				if (nextId.pid == -1)
				{
					return null;
				}

				Page pg = new Page();

				// pin the current page with the new id
				SystemDefs.JavabaseBM.pinPage(nextId, pg, false);

				currentPage = new BTLeafPage(pg, keyType);

				// set currentRid to the new Record in this page
				currentRid = currentPage.firstRecord();

				while (currentRid == null && nextId.pid != -1)
				{
					SystemDefs.JavabaseBM.unpinPage(currentPage.getCurPage(), false);

					SystemDefs.JavabaseBM.pinPage(nextId, currentPage, false);

					currentRid = currentPage.firstRecord();

					nextId = currentPage.getNextPage();
				}
				
				if(currentRid == null)
				{
					SystemDefs.JavabaseBM.unpinPage(currentPage.getCurPage(), false);
					
					currentPage = null;
					
					return null;
				}
			}

			// if not null get the key data entry of the current Rid
			KeyDataEntry keyDataEntry = currentPage.getCurrent(currentRid);

			/**
			 * @Aamer
			 *        if hiKey == null no need for comparison as we scan to the
			 *        end
			 */
			if (hiKey != null)
				// if this key is greater that hi key then return null
				if (BT.keyCompare(keyDataEntry.key, hiKey) > 0)
				{
					currentPage = null;
					
					return null;
				}

			prev = new RID();

			prev.copyRid(currentRid);

			// get the next record of the current record
			RID nextRid = currentPage.nextRecord(currentRid);

			// set the current record to the new one
			currentRid = nextRid;

			return keyDataEntry;
		}
		else
		{
			currentPage = null;
			
			return null;
		}
	}

	/**
	 * Delete currently-being-scanned(i.e., just scanned) data entry.
	 * 
	 * Overrides: delete_current in class IndexFileScan
	 * 
	 * @throws IOException
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws KeyNotMatchException
	 * @throws InvalidSlotNumberException
	 * @throws DeleteRecException
	 */
	@Override
	public void delete_current()
			throws DeleteRecException,
			InvalidSlotNumberException,
			KeyNotMatchException,
			NodeNotMatchException,
			ConvertException,
			IOException
	{

		// KeyDataEntry keyDataEntry = null;

		// keyDataEntry = currentPage.getCurrent(currentRid);

		// currentPage.delEntry(keyDataEntry);

		/**
		 * much better if u use the current rid to delete directly
		 * instead of looking again using delEntry
		 */
		if (flag && prev != null)
		{
			currentPage.deleteSortedRecord(prev);

			if(currentRid != null)
				currentRid.copyRid(prev);

			prev = null;
		}

		/**
		 * @Aamer
		 *        due to deleteSorted record the records are compacted
		 *        using method called compact slots in HFpage so when
		 *        u delete record with respect to value in currentRid
		 *        the following record in line takes this currentRid
		 *        as its value so currentRid points to the next record
		 *        after the deleted one directly
		 */
		// currentRid.copyRid(currentPage.nextRecord(currentRid));

	}

	/**
	 * Returns:
	 * 
	 * the maxumum size of the key in BTFile
	 * 
	 * Overrides: keysize in class IndexFileScan
	 */
	@Override
	public int keysize()
	{

		return keyLength;
	}

	/**
	 * destructor.
	 * 
	 * unpin some pages if they are not unpinned already.
	 * and do some clearing work.
	 * 
	 * @throws IOException
	 * @throws InvalidFrameNumberException
	 * @throws HashEntryNotFoundException
	 * @throws PageUnpinnedException
	 * @throws ReplacerException
	 */
	public void DestroyBTreeFileScan()
			throws ReplacerException,
			PageUnpinnedException,
			HashEntryNotFoundException,
			InvalidFrameNumberException,
			IOException
	{

		flag = false;
		if (currentPage != null)
		{
			SystemDefs.JavabaseBM.unpinPage(currentPage.getCurPage(), true);
		}
	}
}
