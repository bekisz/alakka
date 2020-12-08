package org.alakka


import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import org.alakka.Agent.Message



object Agent {
  sealed trait Message
  sealed trait Request extends Message
  trait Response extends Message

  case class InitRequest( replyTo: ActorRef[Agent.Response] ) extends Request
  case class InitedResponse(replyTo: ActorRef[Agent.Response] ) extends Response

  case class RunRequest( replyTo: ActorRef[Agent.Response] ) extends Request
  case class RunnedResponse(children:Int, replyTo: ActorRef[Agent.Response] ) extends Response

  case class KillRequest( replyTo: ActorRef[Agent.Response] ) extends Request
  case class KilledResponse(replyTo: ActorRef[Agent.Response] ) extends Response

  def apply(): Behavior[Agent.Message] = {
    Behaviors.setup(context => new Agent(context))
  }
}
class Agent (context: ActorContext[Agent.Message]) extends AbstractBehavior[Agent.Message](/*context*/) {

  //import Agent._
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
      case Agent.InitRequest(replyTo) =>
        context.log.debug("Agent Started")
        println(s"${this.context.self.path} : Starting ....")
        replyTo ! Agent.InitedResponse(this.context.self)
        this
      case Agent.RunRequest(replyTo) =>
        context.log.debug("Agent run")
        println(s"${this.context.self.path} : Running ....")
        //replyTo ! Agent.RunnedResponse(this.context.self)
        this

    }
  }
}


