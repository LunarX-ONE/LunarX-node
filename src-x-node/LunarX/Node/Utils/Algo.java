package LunarX.Node.Utils;

public class Algo {
	/**
	 * Search an int array to find the lower_bound index of a specified value.
	 * 
	 * @param a
	 *            the int array to be searched
	 * @param value
	 *            the value to be searched for
	 * @return the index where the first element in the array which does not
	 *         compare less than value
	 */
	public static int lower_bound(int[] a, int value) {
		return lower_bound0(a, 0, a.length, value);
	}

	/**
	 * Search a range of an int array to find the lower_bound index of a
	 * specified value.
	 * 
	 * @param a
	 *            the int array to be searched
	 * @param fromIndex
	 *            the index of the first element (inclusive) to be searched
	 * @param toIndex
	 *            the index of the last element (exclusive) to be searched
	 * @param value
	 *            the value to be searched for
	 * @return the index where the first element in the range
	 *         [fromIndex,toIndex) which does not compare less than value
	 */
	public static int lower_bound(int[] a, int fromIndex, int toIndex, int value) {
		rangeCheck(a.length, fromIndex, toIndex);
		return lower_bound0(a, fromIndex, toIndex, value);
	}

	// Like public version, but without range check
	private static int lower_bound0(int[] a, int fromIndex, int toIndex, int value) {
		int len = toIndex - fromIndex;
		int half, middle;
		while (len > 0) {
			half = len >> 1;
			middle = fromIndex + half;
			if (a[middle] < value) {
				fromIndex = middle;
				++fromIndex;
				len = len - half - 1;
			} else {
				len = half;
			}
		}
		return fromIndex;
	}

	/**
	 * Search an int array to find the upper_index of a specified value.
	 * 
	 * @param a
	 *            the int array to be searched
	 * @param value
	 *            the value to be searched for
	 * @return the index where the first element in array which compares greater
	 *         than value.
	 */
	public static int upper_bound(int a[], int value) {
		return upper_bound0(a, 0, a.length, value);
	}

	/**
	 * Search a range of an int array to find the upper_bound index of a
	 * specified value.
	 * 
	 * @param a
	 * @param fromIndex
	 * @param toIndex
	 * @param value
	 * @return the index where the first element in the range
	 *         [fromIndex,toIndex) which compares greater than value.
	 */
	public static int upper_bound(int a[], int fromIndex, int toIndex, int value) {
		rangeCheck(a.length, fromIndex, toIndex);
		return upper_bound0(a, fromIndex, toIndex, value);
	}

	// Like public version, but without range check
	private static int upper_bound0(int[] a, int fromIndex, int toIndex, int value) {
		int len = toIndex - fromIndex;
		int half, middle;
		while (len > 0) {
			half = len >> 1;
			middle = fromIndex + half;
			if (value < a[middle]) {
				len = half;
			} else {
				fromIndex = middle;
				++fromIndex;
				len = len - half - 1;
			}
		}
		return fromIndex;
	}

	/**
	 * Checks that {@code fromIndex} and {@code toIndex} are in the range and
	 * throws an exception if they aren't.
	 */
	private static void rangeCheck(int arrayLength, int fromIndex, int toIndex) {
		if (fromIndex > toIndex) {
			throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
		}
		if (fromIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(fromIndex);
		}
		if (toIndex > arrayLength) {
			throw new ArrayIndexOutOfBoundsException(toIndex);
		}
	}
}
