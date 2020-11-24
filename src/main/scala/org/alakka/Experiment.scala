package org.alakka

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}


object Experiment {

  // Messages
  trait Message

  // Request Messages
  case class StartCommand(trials: Int = 100) extends Message

  // Response Messages

  //case class ResponseAdapter[T](responseToConvert: T) extends Message

  case class TrialResponseAdapter(responseToConvert: Trial.Response) extends Message

  //extends ResponseAdapter[Trial.Response](responseToConvert: Trial.Response)


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
      case Experiment.StartCommand(trials) =>
        context.log.debug("Experiment Started")
        println("Experiment Started")
        val trialResponseMapper: ActorRef[Trial.Response] =
          context.messageAdapter(trialResponse => Experiment.TrialResponseAdapter(trialResponse))

        (1 to trials)
          .map(i => context.spawn(Trial(), name = s"trial-$i"))
          .map(trial => trial ! Trial.StartRequest(credit = 10, replyTo = trialResponseMapper))

      /*
        for (i <- 1 to trials) {
          val trial = context.spawn(Trial(), s"trial-$i")
          //Experiment.TrialResponseWrapper(trialResponse)


          trial ! Trial.StartRequest(credit = 10, replyTo = trialResponseMapper)
          // this.context.self
          context.log.debug(s"Started ${context.self.path.name} with ${context.self.path}")
          println(s"Started  context.self.path : ${context.self.path.name} with ${context.self.path}")
          println(s"Started  this.context.self.path : ${this.context.self.path.name} with ${this.context.self.path}")

         */



    case Experiment.TrialResponseAdapter(trialResponse)
    =>
    val startedResponse: Trial.StartedResponse = trialResponse.asInstanceOf[Trial.StartedResponse]

    context.log.debug("Trial Response Received")
    println(s"Trial ${startedResponse.replyTo.ref} response received with #${startedResponse.rounds} rounds")


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

