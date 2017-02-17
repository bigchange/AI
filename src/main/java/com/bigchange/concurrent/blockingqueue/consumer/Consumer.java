package com.bigchange.concurrent.blockingqueue.consumer;

import java.util.concurrent.BlockingQueue;

/**
 * 数据消费者线程
 */
public class Consumer implements Runnable {
    BlockingQueue<String> queue; 
    public Consumer(BlockingQueue<String> queue){  
        this.queue = queue;  
    }        
    @Override  
    public void run() {  
        try {  
        	System.out.println(Thread.currentThread().getName());  
            String temp = queue.take(); //如果队列为空，会阻塞当前线程
            System.out.println(temp);  
        } catch (InterruptedException e) {  
            e.printStackTrace();  
        }  
    }  
}  

