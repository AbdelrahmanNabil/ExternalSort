package iterator;

import java.util.Arrays;
import java.util.Random;

import btree.IntegerKey;
import btree.KeyClass;

import diskmgr.Page;
import global.AttrType;
import global.Convert;
import global.PageId;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;

public class test
{

	public static void main(String[] args) throws Exception
	{

		// Convert.class.getName();

		new SystemDefs("TEST", 5000, 5000, "TEST");

		Sort s = new Sort(new AttrType[]{new AttrType(AttrType.attrInteger),new AttrType(AttrType.attrString) }
		, (short)0, null, null, 0, new TupleOrder(TupleOrder.Descending), 4, 5);
		
		Heapfile file = create("new", 3000);
		
		s.begin_exsort("new");
		
		int num = 0;
		
		while(SystemDefs.JavabaseDB.get_file_entry(Sort.NAME + num) != null)
		{
			//print the temp files
			Heapfile temp_file = new Heapfile(Sort.NAME + num);
			Scan scan = temp_file.openScan();
			
			RID rid = new RID();
			Tuple next = scan.getNext(rid);
			
			System.out.println("run " + (num+1));
			while(next != null)
			{
				System.out.println(Convert.getIntValue(0, next.getTupleByteArray()));
				
				next = scan.getNext(rid);
			}
			
			num++;
		}
		
	}

	public static Heapfile create(String filename, int n) throws Exception
	{

		Heapfile file = new Heapfile(filename);

		int[] k = new int[n];
		for (int i = 0; i < n; i++)
		{
			k[i] = i;
		}

		Random ran = new Random();
		int random;
		int tmp;
		for (int i = 0; i < n; i++)
		{
			random = (ran.nextInt()) % n;
			if (random < 0)
				random = -random;
			tmp = k[i];
			k[i] = k[random];
			k[random] = tmp;
		}

		for (int i = 0; i < n; i++)
		{
			random = (ran.nextInt()) % n;
			if (random < 0)
				random = -random;
			tmp = k[i];
			k[i] = k[random];
			k[random] = tmp;
		}

		for (int i = 0; i < n; i++)
		{
			int key = k[i];

			byte[] array = new byte[4];

			Convert.setIntValue(key, 0, array);

			file.insertRecord(array);

		}

		return file;
	}
}

/* 
new SystemDefs("TEST", 5, 5, "TEST");

byte b[] = new byte[1024];

Heapfile file = new Heapfile("A");
file.insertRecord(new byte[]
{ 1, 2, 3, 4 });

for (int i = 0; i < 1024; i++)
	b[i] = (byte) i;

Page pg = new Page(b);

Sort s = new Sort(null, (short) 0, null, null, 0, null, 0, 0);

byte[][] bufs = new byte[1][1024];

bufs[0] = b;

PageId pi[] = new PageId[1];

s.get_buffer_pages(1, pi, bufs);

for (int i = 0; i < 1024; i++)
	bufs[0][i] = (byte) (i + 100);

System.out.println(SystemDefs.JavabaseBM.getNumBuffers());
System.out.println(SystemDefs.JavabaseBM.getNumBuffers()
		- SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
System.out.println();
*/
