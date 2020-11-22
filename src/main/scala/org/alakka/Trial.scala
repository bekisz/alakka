package org.alakka

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}

import org.alakka.Trial.Message



object Trial {
  sealed trait Message
  sealed trait Request extends Message
  trait Response extends Message

  case class StartRequest(credit:Int = 100 /*, replyTo: ActorRef[Trial.Response] */) extends Request
  case class StartedResponse(rounds:Long,replyTo: ActorRef[Trial.Response] ) extends Response

  //final case class GetValue(replyTo: ActorRef[Value]) extends Command
  //final case class Value(n: Int)

  def apply(): Behavior[Trial.Message] = {
    Behaviors.setup(context => new Trial(context))
  }
}
class Trial (context: ActorContext[Trial.Message]) extends AbstractBehavior[Trial.Message](context) {

  //import Trial._
  protected def run(initialCredit :Int) : Long = {
      var round = 0L
    var credit = initialCredit

    while(credit >= 0 ) {
        round=round + 1
        val isHead = scala.util.Random.nextBoolean()
        if (isHead) credit+=1 else credit-=1

      }
      round
  }
  override def onMessage(msg: Message): Behavior[Message] = {

    msg match {
      case Trial.StartRequest(credit) =>
        context.log.debug("Trial Started")
        println(s"${this.context.self.path} : Starting with  $credit credits")
        val roundsAchieved = this.run(credit)
        println(s"${this.context.self.path} : Trial ended at coin toss #$roundsAchieved")
        this

    }
  }
}

