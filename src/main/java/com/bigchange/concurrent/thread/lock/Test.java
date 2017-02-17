package com.bigchange.concurrent.thread.lock;

/**
 * cpu kernel num
 */
public class Test {
	public static void main(String[] args) {
		int num = Runtime.getRuntime().availableProcessors();
		System.out.println(num);
	}

}
