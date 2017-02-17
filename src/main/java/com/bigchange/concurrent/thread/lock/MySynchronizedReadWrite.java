package com.bigchange.concurrent.thread.lock;

/**
 *  无法实现多个线程
 *  同时进行读
 */
public class MySynchronizedReadWrite {
    
    public static void main(String[] args)  {

        final MySynchronizedReadWrite test = new MySynchronizedReadWrite();
         
        new Thread("thread-22"){
            public void run() {
                test.get(Thread.currentThread());
            };
        }.start();
         
        new Thread(){
            public void run() {
                test.get(Thread.currentThread());
            };
        }.start();
         
    }  
     
    public synchronized void get(Thread thread) {
        long start = System.currentTimeMillis();
        while(System.currentTimeMillis() - start <= 1) {
            System.out.println(thread.getName()+" 正在进行读操作");
        }
        System.out.println(thread.getName()+" 读操作完毕");
    }

}
