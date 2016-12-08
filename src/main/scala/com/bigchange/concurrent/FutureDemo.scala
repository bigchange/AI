package com.bigchange.concurrent
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

/**
  * Created by C.J.YOU on 2016/12/7.
  */

/** 执行以下步骤:
  * 研磨所需的咖啡豆
  * 加热一些水
  * 用研磨好的咖啡豆和热水制做一杯咖啡
  * 打奶泡
  * 结合咖啡和奶泡做成卡布奇诺
  */

object FutureDemo {

  type CoffeeBeans = String
  type GroundCoffee = String
  case class Water(temperature: Int)
  type Milk = String
  type FrothedMilk = String
  type Espresso = String
  type Cappuccino = String

  case class GrindingException(msg: String) extends Exception(msg)
  case class FrothingException(msg: String) extends Exception(msg)
  case class WaterBoilingException(msg: String) extends Exception(msg)
  case class BrewingException(msg: String) extends Exception(msg)

  // 创建专用的线程池， 不适用全局的隐式context
  import java.util.concurrent.Executors
  import concurrent.ExecutionContext
  val executorService = Executors.newFixedThreadPool(4)
  val executionContext = ExecutionContext.fromExecutorService(executorService)

  def grind(beans: CoffeeBeans): Future[GroundCoffee] = Future {
    println("start grinding...")
    Thread.sleep(Random.nextInt(2000))
    if (beans == "baked beans") throw GrindingException("are you joking?")
    println("finished grinding...")
    s"ground coffee of $beans"
  }

  def heatWater(water: Water): Future[Water] = Future {

    println("heating the water now")
    Thread.sleep(Random.nextInt(2000))
    println("hot, it's hot!")
    water.copy(temperature = 85)

  }

  def frothMilk(milk: Milk): Future[FrothedMilk] = Future {
    println("milk frothing system engaged!")
    Thread.sleep(Random.nextInt(2000))
    println("shutting down milk frothing system")
    s"frothed $milk"
  }

  def brew(coffee: GroundCoffee, heatedWater: Water): Future[Espresso] = Future {
    println("happy brewing :)")
    Thread.sleep(Random.nextInt(2000))
    println("it's brewed!")
    "espresso"
  }

  def combine(espresso: Espresso, frothedMilk: FrothedMilk): Cappuccino = "cappuccino"

  val tempreatureOkay: Future[Boolean] = heatWater(Water(25)) map { water =>
    println("we're in the future!")
    (80 to 85) contains water.temperature
  }

  def temperatureOkay(water: Water): Future[Boolean] = Future {
    (80 to 85) contains water.temperature
  }

  val flatFuture: Future[Boolean] = heatWater(Water(25)) flatMap {
    water => temperatureOkay(water)
  }

  val nestedFuture: Future[Future[Boolean]] = heatWater(Water(25)) map {
    water => temperatureOkay(water)
  }

  // 准备动作
  def prepareCappuccino(): Future[Cappuccino] = {
    val groundCoffee = grind("arabica beans")
    val heatedWater = heatWater(Water(20))
    val frothedMilk = frothMilk("milk")
    for {
      ground <- groundCoffee
      water <- heatedWater
      foam <- frothedMilk
      espresso <- brew(ground, water)
    } yield combine(espresso, foam)
  }


  def main(args: Array[String]) {

    // 回调
    import scala.util.{Failure, Success}
    grind("arabica beans").onComplete {
      case Success(ground) => println(s"got my $ground")
      case Failure(ex) => println("This grinder needs a replacement, seriously!")
    }

    prepareCappuccino().onComplete {
      case Success(res) => println(res)
      case Failure(ex) => println(ex)
    }


  }

}
