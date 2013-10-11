package iterator;

import global.AttrType;
import global.Convert;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import heap.HFPage;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;

import java.io.IOException;
import java.text.AttributedCharacterIterator.Attribute;
import java.util.ArrayList;

import chainexception.ChainException;
import diskmgr.Page;

/**
 * The Sort class sorts a file. All necessary information are passed as
 * arguments to the constructor. After the constructor call, the user can
 * repeatly call <code>get_next()</code> to get tuples in sorted order. After
 * the sorting is done, the user should call <code>close()</code> to clean up.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class Sort extends Iterator implements GlobalConst {

	private static final int ARBIT_RUNS = 10;
	public static final String NAME = "temp";
	private AttrType[] _in;
	private short n_cols;
	private short[] str_lens;
	private Iterator _am;
	private int _sort_fld;
	private TupleOrder order;
	private int _n_pages;
	private byte[][] bufs;
	private boolean first_time;
	private int Nruns;
	private int max_elems_in_heap;
	private int sortFldLen;
	private int tuple_size;

	// private pnodeSplayPQ Q;
	private Heapfile[] temp_files;
	private int n_tempfiles;
	private Tuple output_tuple;
	private int[] n_tuples;
	private int n_runs;
	private Tuple op_buf;
	// private OBuf o_buf;
	// private SpoofIbuf[] i_buf;
	private PageId[] bufs_pids;
	private boolean useBM = true; // flag for whether to use buffer manager

	/**
	 * Class constructor, take information about the tuples, and set up the
	 * sorting
	 * 
	 * @param in
	 *            array containing attribute types of the relation
	 * @param len_in
	 *            number of columns in the relation
	 * @param str_sizes
	 *            array of sizes of string attributes
	 * @param am
	 *            an iterator for accessing the tuples
	 * @param sort_fld
	 *            the field number of the field to sort on
	 * @param sort_order
	 *            the sorting order (ASCENDING, DESCENDING)
	 * @param sort_field_len
	 *            the length of the sort field
	 * @param n_pages
	 *            amount of memory (in pages) available for sorting
	 * @exception IOException
	 *                from lower layers
	 * @exception SortException
	 *                something went wrong in the lower layer.
	 */
	public Sort(AttrType[] in, short len_in, short[] str_sizes, Iterator am,
			int sort_fld, TupleOrder sort_order, int sort_fld_len, int n_pages)
			throws IOException, SortException {

		_in = in;
		str_lens = str_sizes;
		_sort_fld = sort_fld;
		order = sort_order;
		sortFldLen = sort_fld_len;
		_n_pages = n_pages;

		bufs = new byte[_n_pages][];

		for (int i = 0; i < bufs.length; i++)
			bufs[i] = new byte[GlobalConst.MINIBASE_PAGESIZE];

		bufs_pids = new PageId[_n_pages];
	}

	public void begin_exsort(String heapfile) throws ChainException,
			IOException, Exception {

		begin_phase1(heapfile);
		begin_phase2();
	}

	private void begin_phase1(String heapfile) throws ChainException,
			IOException, Exception {

		// open heap file
		Heapfile file = new Heapfile(heapfile);

		// open scan on file
		Scan scan = file.openScan();

		ArrayList<Heapfile> temp_heapfile_arr = new ArrayList<Heapfile>();

		// create pages
		get_buffer_pages(_n_pages, bufs_pids, bufs);

		HFPage manager = new HFPage();

		RID rid = new RID();

		Tuple tuple = scan.getNext(rid);

		int index = 0;

		ArrayList<SortEntry> arr = new ArrayList<SortEntry>();

		manager.init(bufs_pids[index], new Page(bufs[index]));

		int num = 0;

		while (tuple != null) {
			RID nrid = manager.insertRecord(tuple.getTupleByteArray());

			if (nrid == null) {
				if (index + 1 < bufs.length) {
					index++;

					manager.init(bufs_pids[index], new Page(bufs[index]));

					nrid = manager.insertRecord(tuple.getTupleByteArray());
				} else {
					// sort this run and write it to output
					SortEntry ent_arr[] = new SortEntry[arr.size()];

					arr.toArray(ent_arr);

					QSort.qsort(ent_arr, 0, ent_arr.length - 1);

					Heapfile temp = new Heapfile(NAME + num);
					 
					if (order.tupleOrder == TupleOrder.Ascending)
						for (int i = 0; i < ent_arr.length; i++) {
							manager.setpage(bufs[ent_arr[i].frame]);

							temp.insertRecord(manager.getRecord(ent_arr[i].rid)
									.getTupleByteArray());
						}
					else
						for (int i = ent_arr.length - 1; i >= 0; i--) {
							manager.setpage(bufs[ent_arr[i].frame]);

							temp.insertRecord(manager.getRecord(ent_arr[i].rid)
									.getTupleByteArray());
						}

					temp_heapfile_arr.add(temp);

					num++;

					// clear the array
					arr.clear();

					index = 0;

					manager.init(bufs_pids[index], new Page(bufs[index]));

					nrid = manager.insertRecord(tuple.getTupleByteArray());
				}
			}

			SortEntry ent = get_value_from_bytes(tuple);
			ent.frame = index;
			ent.rid = nrid;

			// insert the value of tuple in arr
			arr.add(ent);

			tuple = scan.getNext(rid);
		}

		if (arr.isEmpty() == false) {
			// write last run
			// sort this run and write it to output
			SortEntry ent_arr[] = new SortEntry[arr.size()];

			arr.toArray(ent_arr);

			QSort.qsort(ent_arr, 0, ent_arr.length - 1);

			Heapfile temp = new Heapfile(NAME + num);

			if (order.tupleOrder == TupleOrder.Ascending)
				for (int i = 0; i < ent_arr.length; i++) {
					manager.setpage(bufs[ent_arr[i].frame]);

					temp.insertRecord(manager.getRecord(ent_arr[i].rid)
							.getTupleByteArray());
				}
			else
				for (int i = ent_arr.length - 1; i >= 0; i--) {
					manager.setpage(bufs[ent_arr[i].frame]);

					temp.insertRecord(manager.getRecord(ent_arr[i].rid)
							.getTupleByteArray());
				}

			temp_heapfile_arr.add(temp);
		}

		n_runs = temp_heapfile_arr.size();

		temp_files = new Heapfile[n_runs];

		temp_heapfile_arr.toArray(temp_files);

		// deallocate created pages
		free_buffer_pages(_n_pages, bufs_pids);
	}

	/**
	 * proc extsort (file)
	 * 
	 * // Given a file on disk, sorts it using three buffer pages // Produce
	 * runs that are B pages long: Pass 0
	 * 
	 * Read B pages into memory, sort them, write out a run. // Merge B-1 runs
	 * at a time to produce longer runs until only // one run (containing all
	 * records of input file) is left
	 * 
	 * While the number of runs at end of previous pass is > 1:
	 * 
	 * // Pass i = 1,2, ... While there are runs to be merged from previous
	 * pass: Choose next B - 1 runs (from previous pass). Read each run into an
	 * input buffer; page at a time. Merge the rUllS and write to the output
	 * buffer; force output buffer to disk one page at a time. endproc
	 * 
	 * @throws ChainException
	 * @throws IOException
	 * @throws Exception
	 */
	private void begin_phase2() throws ChainException, IOException, Exception {
		boolean name_chooser=true;
		while(temp_files.length!=1){//loop until have one runs
		ArrayList<Heapfile> heap_files=new ArrayList<Heapfile>();
		//ArrayList<String> name_heap_files=new ArrayList<String>();
		int last_temp_file =0;
		Tuple temp;
		RID rid=new RID();
		int size=0;
		int num=0;
		Heapfile temp_heap_file;
		while(temp_files.length>last_temp_file){//loop on the heap files
			if(name_chooser){//choose name of heap file
			temp_heap_file=new Heapfile(NAME+"0"+num);
			heap_files.add(temp_heap_file);
			//name_heap_files.add(NAME+"0"+num);
			}
			else{
			temp_heap_file=new Heapfile(NAME+num);
			heap_files.add(temp_heap_file);
			//name_heap_files.add(NAME+num);
			}
		size=Math.min(temp_files.length-last_temp_file,_n_pages);
		last_temp_file+=size;
		Scan [] scan =new Scan[size];
		Tuple [] current=new Tuple[size];
		for(int i=0;i<size;i++){//intialize and opean heap files 
		//temp_files[last_temp_file-size+i]=new Heapfile(heaps_name[last_temp_file-size+i]);//open heap files
		scan [i]=temp_files[last_temp_file-size+i].openScan();// open scan for every heap file
		temp=scan[i].getNext(rid);//set the current array by the first record
		if(temp!=null)
			current[i]=temp;
		else
			current[i]=null;
		}
		int index=-2;
		while(index!=-1){//loop untill arrive to end of sll heap files
		index=-1;
		for(int i=0;i<size;i++){//get the min or max of current array
			if(index==-1&&current[i]!=null){
			index=i;	
			}else{
			if(current[i]!=null){//check if its the end of the scan of i
				if(order.tupleOrder == TupleOrder.Ascending){
					if(get_value_from_bytes(current[i]).compareTo(get_value_from_bytes(current[index]))<0){
						index=i;
					}
				}else{
					if(get_value_from_bytes(current[i]).compareTo(get_value_from_bytes(current[index]))>0){
						index=i;
					}
				}
			}
			}
		}
	if(index==-1){
		//all of current are null end of scan of all runs
		for(int i=last_temp_file-size;i<last_temp_file;i++){
			temp_files[i].deleteFile();
		}
	}else{
		temp_heap_file.insertRecord(current[index].getTupleByteArray());
		current[index]=scan[index].getNext(rid);
	}
	}
		num+=1;
		}
		temp_files=new Heapfile[num];
		//heaps_name=new String[num];
		heap_files.toArray(temp_files);
		//name_heap_files.toArray(heaps_name);
		name_chooser=!name_chooser;
		}
	}

	private SortEntry get_value_from_bytes(Tuple tuple) throws ChainException,
			IOException, Exception {

		SortEntry entry = new SortEntry();

		if (_in[_sort_fld].attrType == AttrType.attrInteger) {
			entry.obj = new Integer(Convert.getIntValue(0,
					tuple.getTupleByteArray()));
		} else if (_in[_sort_fld].attrType == AttrType.attrString) {
			entry.obj = new String(
					Convert.getStrValue(0, tuple.getTupleByteArray(),
							tuple.getTupleByteArray().length));
		} else if (_in[_sort_fld].attrType == AttrType.attrReal) {
			entry.obj = new Double(Convert.getFloValue(0,
					tuple.getTupleByteArray()));
		} else {
			System.out.println("Un defined value");
			System.exit(1);
		}

		return entry;
	}

	/**
	 * Returns the next tuple in sorted order. Note: You need to copy out the
	 * content of the tuple, otherwise it will be overwritten by the next
	 * <code>get_next()</code> call.
	 * 
	 * @return the next tuple, null if all tuples exhausted
	 * @exception IOException
	 *                from lower layers
	 * @exception SortException
	 *                something went wrong in the lower layer.
	 * @exception JoinsException
	 *                from <code>generate_runs()</code>.
	 * @exception UnknowAttrType
	 *                attribute type unknown
	 * @exception LowMemException
	 *                memory low exception
	 * @exception Exception
	 *                other exceptions
	 */
	public Tuple get_next() throws IOException, SortException, UnknowAttrType,
			LowMemException, JoinsException, Exception {

		return null;
	}

	/**
	 * Cleaning up, including releasing buffer pages from the buffer pool and
	 * removing temporary files from the database.
	 * 
	 * @exception IOException
	 *                from lower layers
	 * @exception SortException
	 *                something went wrong in the lower layer.
	 */
	public void close() throws SortException, IOException {

	}

}
