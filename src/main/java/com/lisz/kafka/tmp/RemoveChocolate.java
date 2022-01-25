package com.lisz.kafka.tmp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RemoveChocolate {

	public static void main(String[] args) {
		int n = 7;
		System.out.println(seq(5, 7, 10));
	}

//	private static int ways(int n) {
//		if (n < 3) return 1;
//		int arr[] = new int[n+1];
//		arr[0] = arr[1] = arr[2] = 1;
//		for (int i = 3; i <= n; i++) {
//			arr[i] = arr[i - 1] + arr[i - 3];
//		}
//		return arr[n];
//	}

	private static int ways2(int n) {
		if (n < 3) return 1;
		int a1 = 1;
		int a2 = 1;
		int a3 = 1;
		int a4 = 1;
		for (int i = 3; i <= n; i++) {
			a4 = a1 + a3;
			a1 = a2;
			a2 = a3;
			a3 = a4;
		}
		return a4;
	}

	// 股票题，robinhood ！！！! 　动态规划！
	private static List<Integer> seq(int n, int l, int h) {
		if (h - l + 1 > n - 1) return Arrays.asList(-1);
		List<Integer> res = new ArrayList<>();
		res.add(h - 1);
		res.add(h);
		for (int i = 1; i < n - 1; i++) {
			res.add(h - i);
		}
		return res;
	}
}
