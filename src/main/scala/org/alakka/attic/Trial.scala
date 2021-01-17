package org.alakka.attic

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import org.alakka.attic.Trial.Message


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
class Trial (context: ActorContext[Trial.Message]
             // val examinedPopulation:Long = 1l,
             // val initialPopulation:Long = 1l,
             // val nrOfChildrenProbabilityFunction : ()=> Int,
             // val totalResource:Long = Long.MaxValue
            ) extends AbstractBehavior[Trial.Message](/*context*/) {
  //GwrTrial(context:ActorContext[GwrTrial.Message]) {

  //}
  //import GwrTrial._
  protected def run() : Trial = {

    this
  }
  override def onMessage(msg: Message): Behavior[Message] = {

    msg match {
      case Trial.AgentResponseAdapter(agentResponse: Agent.Response)
      =>
        val initedResponse: Agent.InitedResponse = agentResponse.asInstanceOf[Agent.InitedResponse]

        context.log.debug("GwrTrial Response Received")
        //println(s"GwrTrial ${startedResponse.replyTo.ref} response received with #${startedResponse.rounds} rounds")
        this

    }
  }
}

