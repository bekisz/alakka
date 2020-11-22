package org.alakka

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}




object Experiment {
  trait Message
  trait Status
  case class StartCommand() extends Message


  case class WrappedResponse[T](trialResponse:T) extends Message
  def apply(): Behavior[Message] = {
    Behaviors.setup(context => new Experiment(context))
  }




}
class Experiment(context: ActorContext[Experiment.Message]) extends AbstractBehavior[Experiment.Message](context) {

  //import ._

  //private var n = 0
  //case class TrialResponseWrapper(trialResponse:Trial.Response) extends Experiment.Message


  // var trialResponse = ???

  override def onMessage(msg: Experiment.Message): Behavior[Experiment.Message] = {
    msg match {
      case Experiment.StartCommand() =>
        context.log.debug("Experiment Started")
        println("Experiment Started")
        val trial = context.spawn(Trial(), "trial1")
        //Experiment.TrialResponseWrapper(trialResponse)
        
        val trialResponseMapper : ActorRef[Trial.Response] =
          context.messageAdapter(trialResponse => Experiment.WrappedResponse[Trial.Response](trialResponse))

        trial ! Trial.StartRequest(10, trialResponseMapper)
        // this.context.self
        context.log.debug(s"Started ${context.self.path.name} with ${context.self.path}")
        println(s"Started  context.self.path : ${context.self.path.name} with ${context.self.path}")
        println(s"Started  this.context.self.path : ${this.context.self.path.name} with ${this.context.self.path}")

      case Experiment.WrappedResponse(trialResponse) =>
        val startedResponse:Trial.StartedResponse = trialResponse.asInstanceOf[Trial.StartedResponse]

        context.log.debug("Trial Response Received")
        println(s"Trial response received with #${startedResponse.rounds} rounds")



    }
    this
  }
/*
  override def onMessage(msg: Experiment.Start): Behavior[Experiment.Command] = {
    msg match {
      case Experiment.Start() =>
        context.log.debug("Experiment Started")
        println("Experiment Started")
        this

    }
  } */
}

