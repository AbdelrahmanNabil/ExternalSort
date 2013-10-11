package iterator;

import global.RID;

public class SortEntry<E extends Comparable<E>> implements Comparable<SortEntry<E>>
{

	public E	obj;

	public RID	rid;

	public int	frame;

	@Override
	public int compareTo(SortEntry<E> o)
	{

		return obj.compareTo(o.obj);
	}

	public String toString()
	{

		if (obj instanceof Integer)
			return ((Integer) obj).toString();
		else if (obj instanceof Integer)
			return ((String) obj).toString();
		else
			return "unknown type";
	}
}
