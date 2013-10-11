package btree;

import java.io.IOException;

import diskmgr.*;
import global.*;
import heap.InvalidSlotNumberException;
import heap.Tuple;

/**
 * 
 * @author Hazem
 * 
 */
public class BTLeafPage extends BTSortedPage {

	/**
	 * pin the page with pageno, and get the corresponding BTLeafPage, also it
	 * sets the type to be NodeType.LEAF.
	 * 
	 * Parameters: pageno - Input parameter. To specify which page number the
	 * BTLeafPage will correspond to. keyType - either AttrType.attrInteger or
	 * AttrType.attrString. Input parameter.
	 * 
	 * @param pageno
	 * @param keyType
	 * @throws ConstructPageException
	 * @throws IOException 
	 */
	public BTLeafPage(PageId pageno, int keyType) throws ConstructPageException, IOException 
	{
		// call the parent to pin the page
		super(pageno, keyType);

		// no need for init !

		// set the type of the page

		
		this.setType(NodeType.LEAF);	
	}

	/**
	 * associate the BTLeafPage instance with the Page instance, also it sets
	 * the type to be NodeType.LEAF.
	 * 
	 * Parameters: page - input parameter. To specify which page the BTLeafPage
	 * will correspond to. keyType - either AttrType.attrInteger or
	 * AttrType.attrString. Input parameter.
	 * @throws IOException 
	 */
	public BTLeafPage(Page page, int keyType) throws IOException 
	{
		super(page, keyType);
		
		this.setType(NodeType.LEAF);
	}

	/**
	 * new a page, associate the BTLeafPage instance with the Page instance,
	 * also it sets the type to be NodeType.LEAF.
	 * 
	 * Parameters: keyType - either AttrType.attrInteger or AttrType.attrString.
	 * Input parameter.
	 * 
	 * @param keyType
	 * @throws ConstructPageException
	 * @throws IOException 
	 */
	public BTLeafPage(int keyType) throws ConstructPageException, IOException 
	{
		super(keyType);
	
		this.setType(NodeType.LEAF);	
	}

	/**
	 * insertRecord. READ THIS DESCRIPTION CAREFULLY. THERE ARE TWO RIDs WHICH
	 * MEAN TWO DIFFERENT THINGS. Inserts a key, rid value into the leaf node.
	 * This is accomplished by a call to SortedPage::insertRecord() Parameters:
	 * 
	 * Parameters: key - - the key value of the data record. Input parameter.
	 * dataRid - - the rid of the data record. This is stored on the leaf page
	 * along with the corresponding key value. Input parameter.
	 * 
	 * Returns: the rid of the inserted leaf record data entry, i.e., the pair.
	 * @throws InsertRecException 
	 */
	public RID insertRecord(KeyClass key, RID dataRid) throws InsertRecException 
	{
		// create a leafdata object to take the data required to be inserted in
		// the page
		LeafData leafData = new LeafData(dataRid);

		// create a keyDataEntry to take the key and leaf data and then insert
		// them by calling the insert method of the parent
		KeyDataEntry keyDataEntry = new KeyDataEntry(key, leafData);
		// the second rid to be returned it represents the record of the new
		// data in the current page(slot number , page id)
		RID newRid = null;
		
		newRid = insertRecord(keyDataEntry);

		return newRid;
	}

	/**
	 * Iterators. One of the two functions: getFirst and getNext which provide
	 * an iterator interface to the records on a BTLeafPage.
	 * 
	 * Parameters: rid - It will be modified and the first rid in the leaf page
	 * will be passed out by itself. Input and Output parameter. Returns: return
	 * the first KeyDataEntry in the leaf page. null if no more record getNext
	 * 
	 * @throws IOException
	 * @throws InvalidSlotNumberException
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws KeyNotMatchException
	 */
	public KeyDataEntry getFirst(RID rid) throws IOException,
			InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException 
	{

		Tuple tuple;
		// get the first rid by calling the parent class
		RID firstRid = firstRecord();

		// if rid is null return null
		if (firstRid == null)
			return null;

		// copy the first record to the record passed in the parameters
		rid.copyRid(firstRid);

		// get the tuple of the first record
		tuple = getRecord(rid);
		// get the array of bytes from the tuple
		byte[] array = tuple.getTupleByteArray();
		// create an object keyDataEntry
		KeyDataEntry keyDataEntry = BT.getEntryFromBytes(array, 0,
				array.length, this.keyType, NodeType.LEAF);
		return keyDataEntry;
	}

	/**
	 * Iterators. One of the two functions: getFirst and getNext which provide
	 * an iterator interface to the records on a BTLeafPage.
	 * 
	 * Parameters: rid - It will be modified and the next rid will be passed out
	 * by itself. Input and Output parameter. Returns: return the next
	 * KeyDataEntry in the leaf page. null if no more record. getCurrent
	 * 
	 * @throws IOException
	 * @throws InvalidSlotNumberException
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws KeyNotMatchException
	 */
	public KeyDataEntry getNext(RID rid) throws IOException,
			InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException 
	{
		// get the next record of the current record
		RID nextRid = nextRecord(rid);
		// if null return null
		if (nextRid == null)
			return null;
		// set currentRid to nextRid;
		rid.copyRid(nextRid);
		Tuple tuple;
		tuple = getRecord(rid);
		byte[] array = tuple.getTupleByteArray();
		KeyDataEntry keyDataEntry = BT.getEntryFromBytes(array, 0,
				array.length, this.keyType, NodeType.LEAF);
		return keyDataEntry;
	}

	/**
	 * getCurrent returns the current record in the iteration; it is like
	 * getNext except it does not advance the iterator.
	 * 
	 * Parameters: rid - the current rid. Input and Output parameter. But
	 * Output=Input. Returns: return the current KeyDataEntry delEntry
	 * 
	 * @param rid
	 * @return
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws KeyNotMatchException
	 * @throws IOException
	 * @throws InvalidSlotNumberException
	 */
	public KeyDataEntry getCurrent(RID rid) throws KeyNotMatchException,
			NodeNotMatchException, ConvertException,
			InvalidSlotNumberException, IOException 
	{

		Tuple tuple;
		if (rid == null)
			return null;
		tuple = getRecord(rid);
		byte[] array = tuple.getTupleByteArray();
		KeyDataEntry keyDataEntry = BT.getEntryFromBytes(array,
				tuple.getOffset(), tuple.getLength(), this.keyType,
				NodeType.LEAF);
		return keyDataEntry;

	}

	/**
	 * delete a data entry in the leaf page. Parameters: dEntry - the entry will
	 * be deleted in the leaf page. Input parameter. Returns: true if deleted;
	 * false if no dEntry in the page
	 * 
	 * @param dEntry
	 * @return
	 * @throws DeleteRecException
	 * @throws IOException
	 * @throws InvalidSlotNumberException
	 * @throws ConvertException
	 * @throws NodeNotMatchException
	 * @throws KeyNotMatchException
	 */
	public boolean delEntry(KeyDataEntry dEntry) throws DeleteRecException,
			IOException, InvalidSlotNumberException, KeyNotMatchException,
			NodeNotMatchException, ConvertException 
	{
		// get the first record in the page
		RID rid = firstRecord();
		// get the KeyDataEntry using the getCurrent method
		KeyDataEntry keyDataEntry = getCurrent(rid);
		// loop until keyDataEntry is equal to null
		while (keyDataEntry != null) 
		{
			if (BT.keyCompare(dEntry.key, keyDataEntry.key) == 0) 
			{
				return deleteSortedRecord(rid);
			}
			// assign the keyDataEntry to KeyDataEntry of the next record
			keyDataEntry = getNext(rid);
		}
		// if rid reached null then return false nothing was deleted
		return false;
	}
}
