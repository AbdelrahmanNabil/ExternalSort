package btree;

import global.Convert;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.Tuple;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import chainexception.ChainException;

import diskmgr.Page;

public class BT implements GlobalConst
{

	public static final int keyCompare(KeyClass paramKeyClass1, KeyClass paramKeyClass2) throws KeyNotMatchException
	{

		if (((paramKeyClass1 instanceof IntegerKey)) && ((paramKeyClass2 instanceof IntegerKey)))
		{
			return ((IntegerKey) paramKeyClass1).getKey().intValue() - ((IntegerKey) paramKeyClass2).getKey().intValue();
		}
		if (((paramKeyClass1 instanceof StringKey)) && ((paramKeyClass2 instanceof StringKey)))
		{
			return ((StringKey) paramKeyClass1).getKey().compareTo(((StringKey) paramKeyClass2).getKey());
		}

		throw new KeyNotMatchException(null, "key types do not match");
	}

	public static final int getKeyLength(KeyClass paramKeyClass) throws KeyNotMatchException, IOException
	{

		if ((paramKeyClass instanceof StringKey))
		{
			ByteArrayOutputStream localByteArrayOutputStream = new ByteArrayOutputStream();
			DataOutputStream localDataOutputStream = new DataOutputStream(localByteArrayOutputStream);
			localDataOutputStream.writeUTF(((StringKey) paramKeyClass).getKey());
			return localDataOutputStream.size();
		}
		if ((paramKeyClass instanceof IntegerKey))
			return 4;
		throw new KeyNotMatchException(null, "key types do not match");
	}

	public static final int getDataLength(short paramShort) throws NodeNotMatchException
	{

		if (paramShort == 12)
			return 8;
		if (paramShort == 11)
			return 4;
		throw new NodeNotMatchException(null, "key types do not match");
	}

	public static final int getKeyDataLength(KeyClass paramKeyClass, short paramShort)
			throws KeyNotMatchException,
			NodeNotMatchException,
			IOException
	{

		return getKeyLength(paramKeyClass) + getDataLength(paramShort);
	}

	public static final KeyDataEntry getEntryFromBytes(
			byte[] paramArrayOfByte,
			int paramInt1,
			int paramInt2,
			int paramInt3,
			short paramShort) throws KeyNotMatchException, NodeNotMatchException, ConvertException
	{

		try
		{
			int i;
			Object localObject2;
			if (paramShort == 11)
			{
				i = 4;
				localObject2 = new IndexData(Convert.getIntValue(paramInt1 + paramInt2 - 4, paramArrayOfByte));
			}
			else if (paramShort == 12)
			{
				i = 8;
				RID localRID = new RID();
				localRID.slotNo = Convert.getIntValue(paramInt1 + paramInt2 - 8, paramArrayOfByte);
				localRID.pageNo = new PageId();
				localRID.pageNo.pid = Convert.getIntValue(paramInt1 + paramInt2 - 4, paramArrayOfByte);
				localObject2 = new LeafData(localRID);
			}
			else
			{
				throw new NodeNotMatchException(null, "node types do not match");
			}
			Object localObject1;
			if (paramInt3 == 1)
			{
				localObject1 = new IntegerKey(new Integer(Convert.getIntValue(paramInt1, paramArrayOfByte)));
			}
			else if (paramInt3 == 0)
			{
				localObject1 = new StringKey(Convert.getStrValue(paramInt1, paramArrayOfByte, paramInt2 - i));
			}
			else
			{
				throw new KeyNotMatchException(null, "key types do not match");
			}
			return new KeyDataEntry((KeyClass) localObject1, (DataClass) localObject2);
		}
		catch (IOException localIOException)
		{
			throw new ConvertException(localIOException, "convert faile");
		}

	}

	public static final byte[] getBytesFromEntry(KeyDataEntry paramKeyDataEntry)
			throws KeyNotMatchException,
			NodeNotMatchException,
			ConvertException
	{

		try
		{
			int i = getKeyLength(paramKeyDataEntry.key);
			int j = i;
			if ((paramKeyDataEntry.data instanceof IndexData))
				i += 4;
			else if ((paramKeyDataEntry.data instanceof LeafData))
			{
				i += 8;
			}
			byte[] arrayOfByte = new byte[i];

			if ((paramKeyDataEntry.key instanceof IntegerKey))
			{
				Convert.setIntValue(((IntegerKey) paramKeyDataEntry.key).getKey().intValue(), 0, arrayOfByte);
			}
			else if ((paramKeyDataEntry.key instanceof StringKey))
				Convert.setStrValue(((StringKey) paramKeyDataEntry.key).getKey(), 0, arrayOfByte);
			else
			{
				throw new KeyNotMatchException(null, "key types do not match");
			}
			if ((paramKeyDataEntry.data instanceof IndexData))
			{
				Convert.setIntValue(((IndexData) paramKeyDataEntry.data).getData().pid, j, arrayOfByte);
			}
			else if ((paramKeyDataEntry.data instanceof LeafData))
			{
				Convert.setIntValue(((LeafData) paramKeyDataEntry.data).getData().slotNo, j, arrayOfByte);
				Convert.setIntValue(((LeafData) paramKeyDataEntry.data).getData().pageNo.pid, j + 4, arrayOfByte);
			}
			else
			{
				throw new NodeNotMatchException(null, "node types do not match");
			}
			return arrayOfByte;
		}
		catch (IOException localIOException)
		{
			throw new ConvertException(localIOException, "convert failed");
		}

	}

	public static void printBTree(Object headerPage) throws Exception
	{

		// the page id object of the header page
		PageId header = (PageId) headerPage;
		// queue for used for BFS
		Queue<Integer> queue = new LinkedList<Integer>();
		// a page to load the header page
		HFPage page = new HFPage();
		SystemDefs.JavabaseBM.pinPage(header, page, false);

		/**
		 * reading the key type and root id from the header page
		 */
		RID rid = new RID(header, 2);
		Tuple tuple = page.getRecord(rid);
		int root_pid = Convert.getIntValue(0, tuple.getTupleByteArray());

		rid = new RID(header, 0);
		tuple = page.getRecord(rid);
		int keyType = Convert.getIntValue(0, tuple.getTupleByteArray());

		// unping header after reading
		SystemDefs.JavabaseBM.unpinPage(header, false);
		queue.add(root_pid);
		// loop until queue is emtpy
		while (!queue.isEmpty())
		{

			int currentNode = queue.poll();

			BTSortedPage currentPage = new BTSortedPage(new PageId(currentNode), keyType);
			// if leaf print all leaf pages and then break the loop
			if (currentPage.getType() == NodeType.LEAF)
			{
				printAllLeafPages(headerPage);
				SystemDefs.JavabaseBM.unpinPage(new PageId(currentNode), false);
				break;
			}
			else
			{
				// print the current page
				printPage(new PageId(currentNode), keyType);

				BTIndexPage indexPage = new BTIndexPage(currentPage, keyType);//(BTIndexPage) currentPage;
				RID current_rid = new RID();
				KeyDataEntry keyDataEntry = indexPage.getFirst(current_rid);
				// put all the children of this node in the queue
				while (keyDataEntry != null)
				{
					IndexData data = (IndexData) keyDataEntry.data;
					queue.add(data.getData().pid);
					keyDataEntry = indexPage.getNext(current_rid);
				}
				SystemDefs.JavabaseBM.unpinPage(new PageId(currentNode), false);

			}

		}
	}

	public static void printAllLeafPages(Object headerPage) throws ChainException, IOException
	{
		// the page id object of the header page
		PageId header = (PageId) headerPage;
		
		// a page to load the header page
		HFPage page = new HFPage();
		
		SystemDefs.JavabaseBM.pinPage(header, page, false);

		/**
		 * reading the key type and root id from the header page
		 */
		RID rid = new RID(header, 2);
		Tuple tuple = page.getRecord(rid);
		int root_pid = Convert.getIntValue(0, tuple.getTupleByteArray());

		rid = new RID(header, 0);
		tuple = page.getRecord(rid);
		int keyType = Convert.getIntValue(0, tuple.getTupleByteArray());

		// unping header after reading
		SystemDefs.JavabaseBM.unpinPage(header, false);
		
		HFPage hfpge = new HFPage();
		BTIndexPage indpg;
		PageId firstleaf = new PageId(root_pid);
		
		SystemDefs.JavabaseBM.pinPage(firstleaf, hfpge, false);
		
		//get the first leaf page
		while(hfpge.getType() != NodeType.LEAF)
		{
			indpg = new BTIndexPage(hfpge, keyType);
		
			SystemDefs.JavabaseBM.unpinPage(firstleaf, false);
			
			firstleaf = indpg.getLeftLink();
			
			SystemDefs.JavabaseBM.pinPage(firstleaf, hfpge, false);
		}
		
		SystemDefs.JavabaseBM.unpinPage(firstleaf, false);
		
		printPage(firstleaf, keyType);
		
		PageId next = hfpge.getNextPage();
		
		//loop over the leaf pages
		while(next.pid != -1)
		{
			printPage(next, keyType);
			
			SystemDefs.JavabaseBM.pinPage(next, hfpge, false);
			SystemDefs.JavabaseBM.unpinPage(next, false);
			
			next = hfpge.getNextPage();
		}
	}

	public static void printPage(PageId pageId, int keyType) throws ChainException, IOException
	{

		Page pg = new Page();

		// pin this page
		SystemDefs.JavabaseBM.pinPage(pageId, pg, false);

		// open new HFpage
		HFPage hfpg = new HFPage(pg);

		// determine the type and act on it
		if (hfpg.getType() == NodeType.INDEX)
		{
			// open new BTIndexPage
			BTIndexPage indpg = new BTIndexPage(pg, keyType);
			
			int num = indpg.numberOfRecords();
			
			System.out.print("Index page <" + pageId.pid + "> +" + num + " [");

			RID rid = new RID();
			KeyDataEntry entry = indpg.getFirst(rid);

			if(entry != null)
				System.out.print("<" + entry.key.toString() + ", " + ((IndexData) entry.data).toString() + ">");

			for (int i = 1; i < num; i++)
			{
				entry = indpg.getNext(rid);

				if(entry != null)
					System.out.print("<" + entry.key.toString() + ", " + ((IndexData) entry.data).toString() + ">");
			}

			System.out.print("]");
		}
		else if (hfpg.getType() == NodeType.LEAF)
		{
			// open new BTIndexPage
			BTLeafPage leafpg = new BTLeafPage(pg, keyType);
			
			int num = leafpg.numberOfRecords();
			
			System.out.print("Leaf page <" + pageId.pid + "> +" + num + " [");

			RID rid = new RID();
			KeyDataEntry entry = leafpg.getFirst(rid);

			if(entry != null)
				System.out.print("<" + entry.key.toString() + ", " + ((LeafData) entry.data).toString() + ">");

			for (int i = 1; i < leafpg.numberOfRecords(); i++)
			{
				entry = leafpg.getNext(rid);

				if(entry != null)
					System.out.print("<" + entry.key.toString() + ", " + ((LeafData) entry.data).toString() + ">");
			}

			System.out.print("]");
		}
		else if (hfpg.getType() == NodeType.BTHEAD)
		{
			System.out.print("Header page <" + pageId.pid + "> [");
			
			// get the first record in the page
			RID rid = new RID(pageId, 0);

			// get the tuple from it
			Tuple tuple = hfpg.getRecord(rid);

			// convert it to integer and assign the keyType to it
			System.out.print("<KeyType = " + Convert.getIntValue(0, tuple.getTupleByteArray()) + ">");

			// get the second record in the page
			rid = new RID(pageId, 1);
			tuple = hfpg.getRecord(rid);
			System.out.print("<KeyLength = " + Convert.getIntValue(0, tuple.getTupleByteArray()) + ">");

			// get the third record in the page
			rid = new RID(pageId, 2);
			tuple = hfpg.getRecord(rid);
			System.out.print("<Root = " + Convert.getIntValue(0, tuple.getTupleByteArray()) + ">]");
		}
		else
		{
			System.out.print("page <" + pageId.pid + "> +" + hfpg.getSlotCnt());
		}

		//un pin the page
		SystemDefs.JavabaseBM.unpinPage(pageId, false);
		
		// new line
		System.out.println();

	}

}

/*
 * Location: C:\Users\Aamer\Desktop\dmp\BTree\lib\btreeAssign.jar
 * Qualified Name: btree.BT
 * JD-Core Version: 0.6.0
 */