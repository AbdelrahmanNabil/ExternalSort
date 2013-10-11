package btree;

//import KeyDataEntry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import chainexception.ChainException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidBufferException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

import diskmgr.DiskMgrException;
import diskmgr.DuplicateEntryException;
import diskmgr.FileEntryNotFoundException;
import diskmgr.FileIOException;
import diskmgr.FileNameTooLongException;
import diskmgr.InvalidPageNumberException;
import diskmgr.InvalidRunSizeException;
import diskmgr.OutOfSpaceException;
import diskmgr.Page;

//import global.AttrType;
import global.Convert;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.Tuple;

public class BTreeFile extends IndexFile
{

	/**
	 * must have a pointer to the root page which is initialized at the
	 * beginning in the constructors as empty leaf pages and the tree grows
	 * further when several insertions is done
	 */
	public PageId	root;

	private PageId	headerPage;

	BTFileScan		btFileScan;
	/**
	 * the name of the file
	 */
	private String	name;
	/**
	 * must have an instance which tells us the key type plz note that the
	 * keyType is set using the global constant defined in the AttrType.class
	 * which are
	 * 
	 * AttrType.attrString
	 * AttrType.attrInteger
	 * AttrType.attrReal
	 * AttrType.attrSymbol
	 * AttrType.int attrNull
	 */
	public int		keyType;

	public int		keyLength;

	/**
	 * BTreeFile class an index file with given filename should already exist;
	 * this opens it.
	 * Parameters: filename - the B+ tree file name. Input parameter.
	 * 
	 * @param string
	 * @throws IOException
	 * @throws DiskMgrException
	 * @throws InvalidPageNumberException
	 * @throws FileIOException
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
	public BTreeFile(String filename)
			throws FileIOException,
			InvalidPageNumberException,
			DiskMgrException,
			IOException,
			ReplacerException,
			HashOperationException,
			PageUnpinnedException,
			InvalidFrameNumberException,
			PageNotReadException,
			BufferPoolExceededException,
			PagePinnedException,
			BufMgrException,
			InvalidSlotNumberException
	{

		name = filename;
		// must check if the file is not found ?
		// call get file entry from java db and it will return the first id in
		// the file
		headerPage = SystemDefs.JavabaseDB.get_file_entry(filename);
		// check if the file is not found
		if (headerPage == null)
			throw new FileNotFoundException();
		// read the key type,the id of the root ,the key size back again
		Setter.readAttr(headerPage, this);
		System.out.println("-----------------header Page" + headerPage.pid);
		System.out.println("----------------" + keyType + " " + keyLength + " " + root.pid);
	}

	/**
	 * if index file exists, open it; else create it.
	 * 
	 * Parameters: filename - file name. Input parameter.
	 * keytype - the type of key. Input parameter.
	 * keysize - the maximum size of a key. Input parameter.
	 * delete_fashion - full delete or naive delete. Input parameter.
	 * It is either DeleteFashion.NAIVE_DELETE or DeleteFashion.FULL_DELETE.
	 * 
	 * @return
	 * @throws IOException
	 * @throws DiskMgrException
	 * @throws FileIOException
	 * @throws OutOfSpaceException
	 * @throws DuplicateEntryException
	 * @throws InvalidRunSizeException
	 * @throws InvalidPageNumberException
	 * @throws FileNameTooLongException
	 * @throws BufMgrException
	 * @throws PagePinnedException
	 * @throws BufferPoolExceededException
	 * @throws PageNotReadException
	 * @throws InvalidFrameNumberException
	 * @throws PageUnpinnedException
	 * @throws HashOperationException
	 * @throws ReplacerException
	 * @throws HashEntryNotFoundException
	 * @throws ConstructPageException
	 * @throws InvalidSlotNumberException
	 */
	public BTreeFile(String filename, int keytype, int keysize, int delete_fashion)
			throws FileNameTooLongException,
			InvalidPageNumberException,
			InvalidRunSizeException,
			DuplicateEntryException,
			OutOfSpaceException,
			FileIOException,
			DiskMgrException,
			IOException,
			ReplacerException,
			HashOperationException,
			PageUnpinnedException,
			InvalidFrameNumberException,
			PageNotReadException,
			BufferPoolExceededException,
			PagePinnedException,
			BufMgrException,
			HashEntryNotFoundException,
			ConstructPageException,
			InvalidSlotNumberException
	{

		name = filename;
		headerPage = SystemDefs.JavabaseDB.get_file_entry(filename);

		if (headerPage == null)
		{
			this.keyLength = keysize;
			this.keyType = keytype;

			headerPage = SystemDefs.JavabaseBM.newPage(new Page(), 1);
			// SystemDefs.JavabaseBM.unpinPage(headerPage, false);

			SystemDefs.JavabaseDB.add_file_entry(filename, headerPage);

			// set the root id to the header page id + 1

			BTLeafPage btpge = new BTLeafPage(keyType);

			root = btpge.getCurPage();

			SystemDefs.JavabaseBM.unpinPage(root, true);

		}
		else
		{
			Setter.readAttr(headerPage, this);
		}
		System.out.println("-----------------header Page" + headerPage.pid);
		System.out.println("----------------" + keyType + " " + keyLength + " " + root.pid);
	}

	/**
	 * test
	 * 
	 * @return
	 */
	public PageId getHeaderPage() throws ChainException, IOException
	{

		SystemDefs.JavabaseBM.unpinPage(headerPage, true);

		Setter.writeAttr(headerPage, this);

		return headerPage;
	}

	/**
	 * Close the B+ tree file. Unpin header page.
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
	 */
	public void close()
			throws ReplacerException,
			PageUnpinnedException,
			HashEntryNotFoundException,
			InvalidFrameNumberException,
			HashOperationException,
			PageNotReadException,
			BufferPoolExceededException,
			PagePinnedException,
			BufMgrException,
			IOException
	{

		if (headerPage != null)
		{
			if (btFileScan != null)
			{
				if (btFileScan.currentPage != null)
					System.out.println("warning The scan was opened");

				btFileScan.DestroyBTreeFileScan();
			}

			SystemDefs.JavabaseBM.unpinPage(headerPage, true);

			Setter.writeAttr(headerPage, this);

			SystemDefs.JavabaseBM.unpinPage(headerPage, true);

			// because of the test
			headerPage = null;
		}

	}

	/**
	 * create a scan with given keys Cases:
	 * (1) lo_key = null, hi_key = null scan the whole index
	 * (2) lo_key = null, hi_key!= null range scan from min to the hi_key
	 * (3) lo_key!= null, hi_key = null range scan from the lo_key to max
	 * (4) lo_key!= null, hi_key!= null, lo_key = hi_key exact match (might not
	 * unique)
	 * (5) lo_key!= null, hi_key!= null, lo_key < hi_key
	 * 
	 * range scan from lo_key to hi_key Parameters:
	 * 
	 * lo_key - the key where we begin scanning. Input parameter.
	 * hi_key - the key where we stop scanning. Input parameter.
	 * 
	 * @param lowkey
	 * @param hikey
	 * @return
	 * @throws BufMgrException
	 * @throws PagePinnedException
	 * @throws BufferPoolExceededException
	 * @throws PageNotReadException
	 * @throws InvalidFrameNumberException
	 * @throws PageUnpinnedException
	 * @throws HashOperationException
	 * @throws ReplacerException
	 * @throws HashEntryNotFoundException
	 */
	public BTFileScan new_scan(KeyClass lowkey, KeyClass hikey)
			throws IOException,
			InvalidSlotNumberException,
			NodeNotMatchException,
			ConvertException,
			ConstructPageException,
			KeyNotMatchException,
			DeleteRecException,
			ReplacerException,
			HashOperationException,
			PageUnpinnedException,
			InvalidFrameNumberException,
			PageNotReadException,
			BufferPoolExceededException,
			PagePinnedException,
			BufMgrException,
			HashEntryNotFoundException
	{

		if (headerPage != null)
		{
			btFileScan = new BTFileScan(root.pid, keyType, lowkey, hikey, keyLength);

			return btFileScan;
		}
		else
			return null;
	}

	/**
	 * Destroy entire B+ tree file.
	 * 
	 * @throws InvalidFrameNumberException
	 * @throws HashEntryNotFoundException
	 * @throws PageUnpinnedException
	 * @throws ReplacerException
	 * @throws IOException
	 * @throws DiskMgrException
	 * @throws InvalidPageNumberException
	 * @throws FileIOException
	 * @throws FileEntryNotFoundException
	 * @throws BufMgrException
	 * @throws PagePinnedException
	 * @throws BufferPoolExceededException
	 * @throws PageNotReadException
	 * @throws HashOperationException
	 * @throws ConstructPageException
	 * @throws InvalidSlotNumberException
	 * @throws InvalidBufferException
	 * @throws InvalidRunSizeException
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws KeyNotMatchException
	 */
	public void destroyFile()
			throws ReplacerException,
			PageUnpinnedException,
			HashEntryNotFoundException,
			InvalidFrameNumberException,
			FileEntryNotFoundException,
			FileIOException,
			InvalidPageNumberException,
			DiskMgrException,
			IOException,
			HashOperationException,
			PageNotReadException,
			BufferPoolExceededException,
			PagePinnedException,
			BufMgrException,
			ConstructPageException,
			InvalidSlotNumberException,
			InvalidBufferException,
			InvalidRunSizeException,
			KeyNotMatchException,
			NodeNotMatchException,
			ConvertException
	{

		if (headerPage != null)
		{
			// delete the file from data base
			SystemDefs.JavabaseDB.delete_file_entry(name);

			// here starts the new work
			// queue for used for BFS
			Queue<Integer> queue = new LinkedList<Integer>();

			// unping header after reading
			SystemDefs.JavabaseBM.freePage(headerPage);
			queue.add(root.pid);
			// loop until queue is emtpy
			while (!queue.isEmpty())
			{

				int currentNode = queue.poll();

				BTSortedPage currentPage = new BTSortedPage(new PageId(currentNode), keyType);
				// if leaf print all leaf pages and then break the loop
				if (currentPage.getType() == NodeType.LEAF)
				{

					SystemDefs.JavabaseBM.freePage(new PageId(currentNode));
				}
				else
				{

					BTIndexPage indexPage = new BTIndexPage(currentPage, keyType);// (BTIndexPage)
																					// currentPage;
					RID current_rid = new RID();
					KeyDataEntry keyDataEntry = indexPage.getFirst(current_rid);
					// put all the children of this node in the queue
					while (keyDataEntry != null)
					{
						IndexData data = (IndexData) keyDataEntry.data;
						queue.add(data.getData().pid);
						keyDataEntry = indexPage.getNext(current_rid);
					}
					SystemDefs.JavabaseBM.freePage(new PageId(currentNode));

				}

			}
			
			headerPage = null;
		}
	}

	public void traceFilename(String string)
	{

	}

	/**
	 * insert record with the given key and rid
	 * Parameters:
	 * key - the key of the record. Input parameter.
	 * rid - the rid of the record. Input parameter.
	 * 
	 * Overrides: insert in class IndexFile
	 * 
	 * @param key
	 * @param rid
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
	@Override
	public void insert(KeyClass key, RID rid)
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
			DeleteRecException
	{

		// create new pointer to the root of the btreefile
		PageId temproot = new PageId(root.pid);

		// pass this temproot along with key, and rid
		// to the insert record method
		insertRecord(temproot, new KeyDataEntry(key, new LeafData(rid)));
	}

	private void insertRecord(PageId troot, KeyDataEntry newentry)
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
			DeleteRecException
	{

		/**
		 * this method will recursively look for a place for the new key and
		 * then insert it to the tree file , if no space is found then splitting
		 * and push and copy up techniques are used in our way back from this
		 * recursion else this recursion will insert the record and return back
		 * from recursion without doing any thing else
		 */

		/*
		 * first read the page associated to the root using BTSortedPage
		 * constructor which already pins the page and read the page for me
		 */
		BTSortedPage pge = new BTSortedPage(troot, keyType);

		// check if this page is BTLeafPage
		if (pge.getType() == NodeType.LEAF)
		{
			// create new BTLeafPage and set its instance to the
			// BTSortedPage pge
			BTLeafPage lfpge = new BTLeafPage(pge, keyType);

			// first try inserting the record directly to the leaf page
			RID nrid = lfpge.insertRecord(newentry.key, ((LeafData) newentry.data).getData());

			if (nrid != null)// means the record is inserted successfully
			{
				// set the newentry to null values to know that entry is
				// inserted
				// successfully
				newentry.key = null;
				newentry.data = null;

				// un pin the page before return
				SystemDefs.JavabaseBM.unpinPage(lfpge.getCurPage(), true);

				// return from the recursion without doing any thing else
				return;
			}

			// no space left and we have to deal with the splitting technique
			else
			{
				// get number of records in first node lfpge
				int num = lfpge.numberOfRecords();

				// create new BTLeafPage which is second Node
				BTLeafPage lfpge2 = new BTLeafPage(keyType);

				// create new rid to hold the rid value after skipping num/2
				// records
				RID moverid = new RID();

				// get the first rid in the first node lfpge
				lfpge.getFirst(moverid);

				int i = 0;

				// leave the num/2 records untouched and start moving the
				// num - num/2 records to the second node lfpge2
				for (i = 1; i < num / 2; i++)
				{
					lfpge.getNext(moverid);
				}

				RID delete_from = new RID();

				// copy the rid which we are going to delete from
				delete_from.copyRid(moverid);

				KeyDataEntry entry = null;

				// this entry is used to hold the smallest entry in node 2 which
				// is lfpge2
				KeyDataEntry smallest_entry_in_2 = null;

				// move the rest to lfpge2
				for (i = 0; i < num - num / 2; i++)
				{
					// get the next entry
					entry = lfpge.getNext(moverid);

					// insert this entry to the new node lfpge2
					lfpge2.insertRecord(entry.key, ((LeafData) entry.data).getData());

					if (i == 0)
						smallest_entry_in_2 = entry;
				}

				// get the next entry from delete_from rid
				lfpge.getNext(delete_from);

				// delete the num - num/2 entries from lfpge which is node 1
				for (i = 0; i < num - num / 2; i++)
				{
					// delete the entry
					lfpge.deleteSortedRecord(delete_from);
				}

				/*
				 * insert the newentry to the second node
				 */
				if (BT.keyCompare(newentry.key, smallest_entry_in_2.key) >= 0)
					lfpge2.insertRecord(newentry);
				else
					lfpge.insertRecord(newentry);

				/*
				 * set the key of new entry to the key of smallest_entry we get
				 * from node2 lfpge2 set the data of newentry to the new
				 * IndexData(pageId of the lfpge2)
				 */
				newentry.key = smallest_entry_in_2.key;

				// use Curpage method to get the pageId of the lfpge2
				newentry.data = new IndexData(lfpge2.getCurPage());

				// set the sibling pointers in lfpge and lfpge2

				// set the next of lfpge2 to the next of lfpge
				PageId next = new PageId(lfpge.getNextPage().pid);

				lfpge2.setNextPage(next);

				// set the prev of lfpge2 to the lfpge
				lfpge2.setPrevPage(lfpge.getCurPage());

				// set the next of lfpge to the lfpg2
				lfpge.setNextPage(lfpge2.getCurPage());

				// read the next pageId and set its previous to the lfpg2
				if (next.pid != -1)
				{
					Page page = new Page();
					SystemDefs.JavabaseBM.pinPage(next, page, false);

					HFPage hfpage = new HFPage(page);

					hfpage.setPrevPage(lfpge2.getCurPage());

					SystemDefs.JavabaseBM.unpinPage(next, true);
				}

				/**
				 * note that in the first case only leafpage is found so will
				 * insert directly if there is space, but if there is not space
				 * we have to create new indexpage here as in return it won't
				 * create thing as its first index page created when the root
				 * points to leaf page
				 */
				if (root.pid == lfpge.getCurPage().pid)
				{
					// new indexPage
					BTIndexPage indpge = new BTIndexPage(keyType);

					// insert the newentry in the new index page
					indpge.insertKey(newentry.key, ((IndexData) newentry.data).getData());

					// set the leftlink of indpge to the lfpge
					indpge.setLeftLink(lfpge.getCurPage());

					// set the root pointer to the index pge
					root.pid = indpge.getCurPage().pid;

					/**
					 * no need to set the newentry values to null as its already
					 * last call to this function and will return after that
					 */

					// unpin this page
					SystemDefs.JavabaseBM.unpinPage(indpge.getCurPage(), true);
				}

				// un pin the page before return
				SystemDefs.JavabaseBM.unpinPage(lfpge.getCurPage(), true);
				SystemDefs.JavabaseBM.unpinPage(lfpge2.getCurPage(), true);

				// insert the new child
				return;
			}
		}

		// check if this page is BTIndexPage
		else if (pge.getType() == NodeType.INDEX)
		{
			// create new index page and pass btsortedPage instant to it
			BTIndexPage indpge = new BTIndexPage(pge, keyType);

			// scan the entries in the index page lookin for the first
			// largest key than the <given key>

			// iterate records' rids
			RID rid_i = new RID();

			KeyDataEntry prev = new KeyDataEntry((KeyClass) null, indpge.getLeftLink());
			KeyDataEntry entry = indpge.getFirst(rid_i);

			// max number of records
			int max = indpge.numberOfRecords();

			// get the first key lager than given key
			// to choose the correct subtree
			for (int i = 0; i < max && BT.keyCompare(entry.key, newentry.key) <= 0; i++)
			{
				// set the prev pointer
				prev.key = entry.key;
				prev.data = entry.data;

				entry = indpge.getNext(rid_i);
			}

			// call the insert method recursively to insert the newentry
			// with the choosen pageid we get
			insertRecord(((IndexData) prev.data).getData(), newentry);

			if (newentry.key == null || newentry.data == null)
			{
				// un pin the page before return
				SystemDefs.JavabaseBM.unpinPage(indpge.getCurPage(), true);

				// means successful insertion
				return;
			}
			/*
			 * child has been split and now we insert the new smallest value
			 * which comes from last created node2 to the current index page if
			 * it has space note that this could means copy up if node2 is
			 * leafnode or push up if node2 is index node
			 */
			else
			{
				RID nrid = indpge.insertKey(newentry.key, ((IndexData) newentry.data).getData());

				if (nrid != null)
				{
					// means its inserted successfully, so set newentry to null
					newentry.key = null;
					newentry.data = null;

					// un pin the page before return
					SystemDefs.JavabaseBM.unpinPage(indpge.getCurPage(), true);

					// return without doing any thing else
					return;
				}
				/*
				 * no space left so we have to use the splitting technique
				 */
				else
				{
					// get number of records in first node indpge
					int num = indpge.numberOfRecords();

					// create new BTIndexPage which is second Node
					BTIndexPage indpge2 = new BTIndexPage(keyType);

					// create new rid to hold the rid value after skipping num/2
					// records
					RID moverid = new RID();

					// get the first rid in the first node lfpge
					indpge.getFirst(moverid);

					int i = 0;

					// leave the num/2 records untouched and start moving the
					// num - num/2 - 1 records to the second node indpge2
					for (i = 1; i < num / 2; i++)
					{
						indpge.getNext(moverid);
					}

					RID delete_from = new RID();

					// copy the rid which we are going to delete from
					delete_from.copyRid(moverid);

					KeyDataEntry tentry = null;

					// this entry is used to hold the smallest entry
					KeyDataEntry smallest_entry_in_2 = indpge.getNext(moverid);

					// move the rest to indpge2
					for (i = 1; i < num - num / 2; i++)
					{
						// get the next entry
						tentry = indpge.getNext(moverid);

						// insert this entry to the new node indpge2
						indpge2.insertKey(tentry.key, ((IndexData) tentry.data).getData());
					}

					// delete the num - num/2 entries from indpge which is node
					// 1

					// get the next entry from delete_from rid
					indpge.getNext(delete_from);

					for (i = 0; i < num - num / 2; i++)
					{
						// delete the entry
						indpge.deleteSortedRecord(delete_from);
					}

					// set the left link of the indpg2
					indpge2.setLeftLink(((IndexData) smallest_entry_in_2.data).getData());

					/*
					 * insert the newentry to the second node
					 */
					if (BT.keyCompare(newentry.key, smallest_entry_in_2.key) >= 0)
						indpge2.insertRecord(newentry);
					else
						indpge.insertRecord(newentry);

					/*
					 * set the key of new entry to the key of smallest_entry set
					 * the data of newentry to the new IndexData(pageId of the
					 * indpge2)
					 */
					newentry.key = smallest_entry_in_2.key;

					// use Curpage method to get the pageId of the lfpge2
					newentry.data = new IndexData(indpge2.getCurPage());

					/**
					 * if the root is the splitted page
					 */
					if (root.pid == indpge.getCurPage().pid)
					{
						// new indexPage
						BTIndexPage rootindpge = new BTIndexPage(keyType);

						// insert the newentry in the new index page
						rootindpge.insertKey(newentry.key, ((IndexData) newentry.data).getData());

						// set the leftlink of indpge to the lfpge
						rootindpge.setLeftLink(indpge.getCurPage());

						// set the root pointer to the index pge
						root.pid = rootindpge.getCurPage().pid;

						/**
						 * no need to set the newentry values to null as its
						 * already last call to this function and will return
						 * after that
						 */

						// un pin this page
						SystemDefs.JavabaseBM.unpinPage(rootindpge.getCurPage(), true);
					}

					// un pin the page before return
					SystemDefs.JavabaseBM.unpinPage(indpge.getCurPage(), true);
					SystemDefs.JavabaseBM.unpinPage(indpge2.getCurPage(), true);

					// insert the new child
					return;
				}
			}
		}

		// another type other than the two listed above is unacceptable
		// so throw an error
		else
		{
			throw new InsertException("Error in BTreeFile.insert");
		}
	}

	/**
	 * delete leaf entry given its pair. `rid' is IN the data entry; it is not
	 * the id of the data entry)
	 * Parameters: key - the key in pair . Input Parameter.
	 * rid - the rid in pair . Input Parameter.
	 * 
	 * Returns: true if deleted. false if no such record.
	 * 
	 * Throws: DeleteFashionException
	 * neither full delete nor naive delete
	 * 
	 * Throws: LeafRedistributeException
	 * redistribution error in leaf pages
	 * 
	 * Throws: RedistributeException
	 * redistribution error in index pages
	 * 
	 * Throws: InsertRecException
	 * error when insert in index page
	 * 
	 * Throws: KeyNotMatchException
	 * key is neither integer key nor string key
	 * 
	 * Throws: UnpinPageException
	 * error when unpin a page
	 * 
	 * Throws: IndexInsertRecException
	 * error when insert in index page
	 * 
	 * Throws: FreePageException
	 * error in BT page constructor
	 * 
	 * Throws: RecordNotFoundException
	 * error delete a record in a BT page
	 * 
	 * Throws: PinPageException
	 * error when pin a page
	 * 
	 * Throws: IndexFullDeleteException
	 * fill delete error
	 * 
	 * Throws: LeafDeleteException
	 * delete error in leaf page
	 * 
	 * Throws: IteratorException
	 * iterator error
	 * 
	 * Throws: ConstructPageException
	 * error in BT page constructor
	 * 
	 * Throws: DeleteRecException
	 * error when delete in index page
	 * 
	 * Throws: IndexSearchException
	 * error in search in index pages
	 * 
	 * Throws: IOException
	 * error from the lower layer
	 * 
	 * Overrides: Delete in class IndexFile
	 * 
	 * @param key
	 * @param rid
	 * 
	 * @return
	 * 
	 * @throws IOException
	 * @throws KeyNotMatchException
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws InvalidSlotNumberException
	 * @throws ConstructPageException
	 * @throws DeleteRecException
	 * @throws InvalidFrameNumberException
	 * @throws HashEntryNotFoundException
	 * @throws PageUnpinnedException
	 * @throws ReplacerException
	 */
	@Override
	public boolean Delete(KeyClass key, RID rid)
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
			InvalidFrameNumberException
	{

		// obj tht must be del
		KeyDataEntry kdentry = new KeyDataEntry(key, rid);
		return del(new PageId(root.pid), kdentry);
	}

	/**
	 * delete recursively note that this method will delete repeated
	 * elements with same value
	 * 
	 * @param troot
	 * @param kdentry
	 * @return
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
	 */
	private boolean del(PageId troot, KeyDataEntry kdentry)
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
			InvalidFrameNumberException
	{

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

			boolean deleted = false;

			for (int i = 0; i < max; i++)
			{
				if (BT.keyCompare(entry.key, kdentry.key) == 0)
				{
					// found
					// lfpge.delEntry(kdentry);

					/**
					 * we could use this method better to minimize the search
					 * operation again
					 */
					lfpge.deleteSortedRecord(recid);

					deleted = true;

					if (lfpge.numberOfRecords() > recid.slotNo)
						entry = lfpge.getCurrent(recid);
					else
					{
						// un pin the page before return
						SystemDefs.JavabaseBM.unpinPage(lfpge.getCurPage(), true);

						return deleted;// found and deleted
					}
				}
				else if (BT.keyCompare(entry.key, kdentry.key) > 0)
				{
					// un pin the page before return
					SystemDefs.JavabaseBM.unpinPage(lfpge.getCurPage(), true);

					return deleted;// found and deleted
				}
				else
					entry = lfpge.getNext(recid);// try to search in the next
													// record
			}// for loop

			// un pin the page before return
			SystemDefs.JavabaseBM.unpinPage(lfpge.getCurPage(), true);

			// not found
			return false;
		}

		// check if this page is BTIndexPage
		else if (pge.getType() == NodeType.INDEX)
		{
			BTIndexPage indpge = new BTIndexPage(pge, keyType);

			// iterate records rids
			RID rid_i = new RID();

			KeyDataEntry prev = new KeyDataEntry((KeyClass) null, indpge.getLeftLink());// left
																						// most
			KeyDataEntry entry = indpge.getFirst(rid_i);// right

			// max no of rcrds in page
			int max = indpge.numberOfRecords();

			// get the first key lager than given key
			// to choose the correct subtree
			for (int i = 0; i < max && BT.keyCompare(entry.key, kdentry.key) <= 0; i++)
			{
				// set the prev pointer
				prev.key = entry.key;
				prev.data = entry.data;

				entry = indpge.getNext(rid_i);
			}

			// un pin the page before return
			SystemDefs.JavabaseBM.unpinPage(indpge.getCurPage(), true);

			return del(((IndexData) prev.data).getData(), kdentry);
		}

		return false;
	}
}
