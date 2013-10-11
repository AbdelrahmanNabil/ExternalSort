package btree;

import global.PageId;
import global.RID;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;

import diskmgr.Page;

public class BTIndexPage extends BTSortedPage{

	/**
	 * 1- pin the page with pageno, and get the corresponding BTIndexPage, 
	 * 2- also it sets the type of node to be NodeType.INDEX. 
	 * 
	 * Parameters: 
	 * 
	 * pageno - Input parameter. 
	 * 		To specify which page number the BTIndexPage will correspond to. 
	 * 
	 * keyType - either AttrType.attrInteger or AttrType.attrString. 
	 * 		Input parameter.
	 * 
	 * @throws ConstructPageException 
	 * @throws IOException 
	 * */
	public BTIndexPage(PageId pageno, int keyType) throws ConstructPageException, 
															IOException 
	{
		/*
		 * call the super class which do the following
		 * 
		 * 1- pinPage(paramPageId, this, false)
		 * 2- this.keyType = paramInt    
		 *
		 */
		super(pageno, keyType);

		/*
		 * no need to call the init() method of the super class HFpage
		 * to init() the new page with prev, curr, next and extra
		 * data in this new page, note that this method doesn't 
		 * set the type you have to do it manually 
		 */

		//set the type of this page which is indexPage
		setType(NodeType.INDEX);
	}

	/**
	 * 1- associate the BTIndexPage instance with the Page instance, 
	 * 2- also it sets the type of node to be NodeType.INDEX.
	 * 
	 * Parameters: 
	 * 
	 * page - input parameter. 
	 * 		To specify which page the BTIndexPage will correspond to.
	 * 
	 * keyType - either AttrType.attrInteger or AttrType.attrString. 
	 * 		Input parameter.
	 * 
	 * Note that: this method assumes that the page is already initiated 
	 * using the HFpage.init() and then it's passed to this constructor
	 * to do the above listed actions above
	 * 
	 * @throws IOException  
	 */
	public BTIndexPage(Page page, int keyType) throws IOException 
	{

		/*
		 * super(paramPage)
		 * 		-> HFPage.data = paramPage.getpage()
		 * 
		 * this.keyType = paramInt
		 */
		super(page, keyType);

		//set the type of this page which is indexPage
		setType(NodeType.INDEX);
	}

	/**
	 * 1- this method creates one new page and set it as current instant page
	 * 2- also it sets the type of node to be NodeType.INDEX.
	 * 
	 * @param keyType either AttrType.attrInteger or AttrType.attrString. 
	 * 		Input parameter.
	 * 
	 * @throws ConstructPageException
	 * @throws IOException
	 */
	public BTIndexPage(int keyType) throws ConstructPageException, 
											IOException 
	{
		/*
		 * call the super class which do the following
		 * 
		 * Page localPage = new Page();
		 *		
		 *		creates one new page 
		 * PageId localPageId = SystemDefs.JavabaseBM.newPage(localPage, 1);
		 *
		 *		call the HFpage.init() to init() the new page note that method
		 *		init() already sets the current instance to the new created page
		 *		which is passed as parameter in init() method
		 * super.init(localPageId, localPage);
		 * 
		 * this.keyType = paramInt;
		 */
		super(keyType);

		//set the type of this page which is indexPage
		setType(NodeType.INDEX);
	}

	/**
	 * 1- It inserts a value into the index page, 
	 * 
	 * Parameters: key - the key value in . Input parameter. 
	 * pageNo - the pageNo in . Input parameter. 
	 * 
	 * Returns:
	 * 		It returns the rid where the record is inserted; 
	 * 		null if no space left.
	 * 
	 * @throws InsertRecException 
	 */
	public RID insertKey(KeyClass key, PageId pageNo) throws InsertRecException 
	{
		/*
		 * first create new indexData object which is derived 
		 * class of the super class data class and pass to its
		 * constructor the given pageId
		 */
		DataClass idata = new IndexData(pageNo);

		/*
		 * second create new keyDataEntry object which takes
		 * KeyClass and DataClass <- indexData as parameters 
		 * to its constructor
		 */
		KeyDataEntry kdentry = new KeyDataEntry(key, idata);
		
		/*
		 * call the super.insertRecord method which inserts
		 * records in a sorted order and pass to it the 
		 * new created keyDataEntry
		 */
		RID rid = insertRecord(kdentry);
		
		/*
		 * return the rid either null or not null
		 */
		return rid;
	}

	/**
	 * this method returns the pageId associated with this key
	 * meaning the pageId which this key points to
	 * 
	 * @param key
	 * @return
	 * @throws IOException 
	 * @throws ConvertException 
	 * @throws NodeNotMatchException 
	 * @throws KeyNotMatchException 
	 * @throws InvalidSlotNumberException 
	 */
	public PageId getPageNoByKey(KeyClass key) throws InvalidSlotNumberException, 
														KeyNotMatchException, 
														NodeNotMatchException, 
														ConvertException, 
														IOException 
	{
		/*
		 * use the iterator's methods to iterate on the records
		 * and scan them to get this key if it exists and return 
		 * the pageId it points to when this key is found
		 */
		
		//create new rid object which will carry the first rid in
		//this page
		RID rid_iterate = new RID();
		
		//get the first keydataEntry in this current page
		KeyDataEntry kdentry = getFirst(rid_iterate);
		
		/*
		 * loop on the records while the keyClass is found
		 */
		while(BT.keyCompare(key, kdentry.key) != 0)
		{
			//get the next record
			kdentry = getNext(rid_iterate);
			
			//key not found
			if(kdentry == null)
				return null;
		}
		
		//return the pageId after the correct entry is found
		return ((IndexData)kdentry.data).getData();
	}

	/**
	 * Iterators. 
	 * One of the two functions: getFirst and getNext
	 * 		which provide an iterator interface to the records on a BTIndexPage. 
	 * 
	 * Parameters: rid -
	 * 		It will be modified and the first rid in the index page will be passed
	 * 		out by itself. Input and Output parameter. 
	 * 
	 * Returns: return the first KeyDataEntry in the index page. 
	 * 			null if NO MORE RECORD
	 * 
	 * @throws IOException 
	 * @throws InvalidSlotNumberException 
	 * @throws ConvertException 
	 * @throws NodeNotMatchException 
	 * @throws KeyNotMatchException 
	 */
	public KeyDataEntry getFirst(RID rid) throws IOException, 
													InvalidSlotNumberException, 
													KeyNotMatchException, 
													NodeNotMatchException, 
													ConvertException 
	{
		/*
		 * call the HFpage.firstRecord() to get the first 
		 * record in this current page 
		 */
		RID firstrid = firstRecord();
		
		//return null if first rid is null
		if(firstrid == null)
			return null;
		
		/**
		 * set the input output parameter rid to the firstRid
		 */
		rid.copyRid(firstrid);
		
		/*
		 * get the record which is associated to this rid
		 * note that this record is returned in the tuple 
		 * object and its a copy of the real record no need
		 * to get the original one
		 */
		Tuple firstrecord = getRecord(firstrid);
		
		//get the record bytes 
		byte[] record = firstrecord.getTupleByteArray();
		
		/*
		 * now convert this tuple back to our keyDataEntry object
		 * using the BT.getEntryFromByte() method 
		 */
		KeyDataEntry kdentry = 
			
								//get a copy of the bytes in the tuple and pass it 
								//to the constructor
			BT.getEntryFromBytes(record, 
					
								 //set the offset from zero as we work on the whole
								 //array of the tuple
								 0, 
								 
								 //work on the whole record length
								 record.length, 
								 
								 //use the instant keyType
								 this.keyType, 
								 
								 //set the node type which is index as we here in index data
								 NodeType.INDEX);
		
		
		//return the kdentry Object
		return kdentry;

	}

	/**
	 * Iterators. 
	 * One of the two functions: getFirst and getNext which provide
	 * an iterator interface to the records on a BTIndexPage. 
	 * 
	 * Parameters: rid - It will be modified and next rid will be passed out by itself. 
	 * 		Input and Output parameter. 
	 * 
	 * Returns: return the next KeyDataEntry in the index page. 
	 * 		null if no more record
	 * 
	 * @throws IOException 
	 * @throws InvalidSlotNumberException 
	 * @throws ConvertException 
	 * @throws NodeNotMatchException 
	 * @throws KeyNotMatchException 
	 */
	public KeyDataEntry getNext(RID rid) throws IOException, 
													InvalidSlotNumberException, 
													KeyNotMatchException, 
													NodeNotMatchException, 
													ConvertException 
	{
		
		/*
		 * use the given rid to get the next rid after this one 
		 * using the HFpage.nextRecord(RID rid)
		 */
		RID nextrid = nextRecord(rid);
		
		//return null if nextrid is null
		if(nextrid == null)
			return null;
		
		/**
		 * set the input output parameter rid to the nextRid
		 */
		rid.copyRid(nextrid);
		
		/*
		 * get the record which is associated to this rid
		 * note that this record is returned in the tuple 
		 * object and its a copy of the real record no need
		 * to get the original one
		 */
		Tuple nextrecord = getRecord(nextrid);
		
		//get the record bytes 
		byte[] record = nextrecord.getTupleByteArray();
		
		/*
		 * now convert this tuple back to our keyDataEntry object
		 * using the BT.getEntryFromByte() method 
		 */
		KeyDataEntry kdentry = 
			
								//get a copy of the bytes in the tuple and pass it 
								//to the constructor
			BT.getEntryFromBytes(record, 
					
								 //set the offset from zero as we work on the whole
								 //array of the tuple
								 0, 
								 
								 //work on the whole record length
								 record.length, 
								 
								 //use the instant keyType
								 this.keyType, 
								 
								 //set the node type which is index as we here in index data
								 NodeType.INDEX);
		
		
		//return the kdentry Object
		return kdentry;

	}

	/**
	 * Left Link You will recall that the index pages have a left-most pointer
	 * that is followed whenever the search key value is less than the least key
	 * value in the index node. 
	 * 	
	 * 		The previous page pointer is used to implement the left link. 
	 *
	 * Returns: It returns the left most link.
	 * 
	 * @throws IOException 
	 */
	public PageId getLeftLink() throws IOException 
	{
		/*
		 * call the HFpage.getPrevPage to return the left most link
		 */
		return getPrevPage();
	}

	/**
	 * You will recall that the index pages have a left-most pointer that is
	 * followed whenever the search key value is less than the least key value
	 * in the index node. 
	 * 
	 * 		The previous page pointer is used to implement the left link. 
	 * 
	 * The function sets the left link. 
	 * 
	 * Parameters: left - the PageId of the left link you wish to set. Input parameter.
	 * 
	 * @throws IOException 
	 */
	public void setLeftLink(PageId left) throws IOException 
	{
		/*
		 * call the HFpage.setPrevPage to set the left most link
		 */
		setPrevPage(left);
	}

}
