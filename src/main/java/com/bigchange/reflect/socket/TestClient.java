package com.bigchange.reflect.socket;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * 客户端段远程调用实例
 */
public class TestClient {

	public static void main(String[] args) throws Exception {

		Socket socket = new Socket("localhost", 9898);
		OutputStream out = socket.getOutputStream();
		InputStream in = socket.getInputStream();
		
		PrintWriter pw = new PrintWriter(new BufferedOutputStream(out));
		pw.println("com.bigchange.reflect.socket.TestBusiness:getPrice:yifu");
		pw.flush();

		// 阻塞式获取 输入流数据
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String readLine = br.readLine();
		
		System.out.println("client get result: " + readLine);

		socket.close();

	}
}
