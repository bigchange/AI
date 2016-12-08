package com.bigchange.metrics

import java.io.File
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{CsvReporter, MetricRegistry}

/**
  * Created by C.J.YOU on 2016/12/8.
  */
//
object MetricsForApp {

  private val metrics = new MetricRegistry()


  def startReport() = {

    val reporter = CsvReporter.forRegistry(metrics)
      .formatFor(Locale.US)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build(new File("~/projects/data/"))
    reporter.start(1, TimeUnit.SECONDS)

  }
  def main(args: Array[String]) {

    startReport()
    val requests = metrics.meter("requests")
    requests.mark()

  }

}
