package org.alakka

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import org.alakka.Trial.{Message}


object Trial {
  sealed trait Message
  trait Response extends Message


  case class AgentResponseAdapter(responseToConvert: Agent.Response) extends Response

  //final case class GetValue(replyTo: ActorRef[Value]) extends Command
  //final case class Value(n: Int)

  def apply(): Behavior[Trial.Message] = {
    Behaviors.setup(context => new Trial(context))
  }
}
class Trial (context: ActorContext[Trial.Message],
             // val examinedPopulation:Long = 1l,
             // val initialPopulation:Long = 1l,
             // val nrOfChildrenProbabilityFunction : ()=> Int,
             // val maxPopulation:Long = Long.MaxValue
            ) extends AbstractBehavior[Trial.Message](context) {
  //Trial(context:ActorContext[Trial.Message]) {

  //}
  //import Trial._
  protected def run() : Trial = {

    this
  }
  override def onMessage(msg: Message): Behavior[Message] = {

    msg match {
      case Trial.AgentResponseAdapter(agentResponse: Agent.Response)
      =>
        val initedResponse: Agent.InitedResponse = agentResponse.asInstanceOf[Agent.InitedResponse]

        context.log.debug("Trial Response Received")
        //println(s"Trial ${startedResponse.replyTo.ref} response received with #${startedResponse.rounds} rounds")
        this

    }
  }
}

