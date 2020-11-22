package org.alakka

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}


object Experiment {
  trait Command
  trait Status
  case class StartCommand() extends Command
  case class RunningStatus() extends Status {

  }
  def apply(): Behavior[Command] = {
    Behaviors.setup(context => new Experiment(context))
  }




}
class Experiment(context: ActorContext[Experiment.Command]) extends AbstractBehavior[Experiment.Command](context) {

  //import ._

  //private var n = 0
  override def onMessage(msg: Experiment.Command): Behavior[Experiment.Command] = {
    msg match {
      case Experiment.StartCommand() =>
        context.log.debug("Experiment Started")
        println("Experiment Started")
        context.log.debug(s"Started ${context.self.path.name} with ${context.self.path}")
        println(s"Started  context.self.path : ${context.self.path.name} with ${context.self.path}")
        println(s"Started  this.context.self.path : ${this.context.self.path.name} with ${this.context.self.path}")

        this
    }

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

