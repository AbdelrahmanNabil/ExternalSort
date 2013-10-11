package btree;

import global.AttrType;
import global.Convert;
import global.PageId;
import global.SystemDefs;

public class test
{

	public static void main(String[] args) throws Exception
	{

		SystemDefs sysdef = new SystemDefs("BTREE" + ".minibase-db", 5000, 5000, "Clock");

		// create a hfpage object
		/*
		 * HFPage page = new HFPage();
		 * 
		 * PageId pageid = SystemDefs.JavabaseBM.newPage(page, 1);
		 * 
		 * 
		 * page.init(pageid, page);
		 * 
		 * page.setType(NodeType.BTHEAD);
		 */

		BTSortedPage page = new BTSortedPage(1);

		page.insertRecord(new KeyDataEntry(9, new PageId(13)));
		page.insertRecord(new KeyDataEntry(1, new PageId(13)));
		page.insertRecord(new KeyDataEntry(3, new PageId(13)));
		page.insertRecord(new KeyDataEntry(2, new PageId(13)));
		page.insertRecord(new KeyDataEntry(4, new PageId(13)));
		page.insertRecord(new KeyDataEntry(7, new PageId(13)));

		// create a byte array of length 4 bytes = 32/8
		byte[] tuple1 = new byte[4];

		// convert the keyType to byte array
		Convert.setIntValue(4, 0, tuple1);

		// insert the byte array into the header page
		page.insertRecord(tuple1);

		byte[] tuple2 = new byte[4];
		Convert.setIntValue(5, 0, tuple2);
		page.insertRecord(tuple2);

		byte[] tuple3 = new byte[4];
		Convert.setIntValue(6, 0, tuple3);
		page.insertRecord(tuple3);

		// 12 new classes
		BT.class.getName();
		BTSortedPage.class.getName();

		IndexData.class.getName();
		LeafData.class.getName();

		IntegerKey.class.getName();
		StringKey.class.getName();

		KeyDataEntry.class.getName();

		NodeType.class.getName();

		AttrType.class.getName();

		// 4 abstract classes
		DataClass.class.getName();
		KeyClass.class.getName();
		IndexFile.class.getName();
		IndexFileScan.class.getName();

		// 6 exception class
		InsertRecException.class.getName();
		DeleteRecException.class.getName();
		ConstructPageException.class.getName();
		ConvertException.class.getName();
		KeyNotMatchException.class.getName();
		NodeNotMatchException.class.getName();
	}
}
