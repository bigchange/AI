package com.bigchange.thread

/**
  * Created by C.J.YOU on 2017/1/19.
  */
object ThreadPool {

  // 创建专用的线程池， 不适用全局的隐式context
  import java.util.concurrent.Executors

  import concurrent.ExecutionContext
  val executorService = Executors.newFixedThreadPool(4)
  val executionContext = ExecutionContext.fromExecutorService(executorService)


}
