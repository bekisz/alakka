package org.alakka
// Entry point for creating all kind of Monte Carlo-based Artifical Life simpulations
// Even more Comments

import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}

object ExperimentController {
  trait Message
  trait Command extends Message
  trait Response extends Message
  trait Broadcast extends Message

  case class StartCommand() extends Command
  case class ExperimentControlStatus() extends Response with Experiment.Status
  def apply(): Behavior[Message] = {
    Behaviors.setup(context => new ExperimentController(context))

  }
  def main(args:Array[String]) :Unit = {
    val experimentController: ActorSystem[ExperimentController.Message]
    = ActorSystem(ExperimentController(), "ExperimentController")

    experimentController ! ExperimentController.StartCommand()

    //experiment.terminate()

  }

}
class ExperimentController(context: ActorContext[ExperimentController.Message])
  extends AbstractBehavior[ExperimentController.Message](context) {

  override def onMessage(msg: ExperimentController.Message): Behavior[ExperimentController.Message] = {
    msg match {
      case ExperimentController.StartCommand() =>
        val experiment = context.spawn(Experiment(), "myExperiment")
        experiment ! Experiment.StartCommand()

        //context.log.debug(s"Started ${experiment.path.name} with ${experiment.path}")
        println(s"-In ExprimentController onMessage:")

        println(s"Started  context.self.path : ${context.self.path.name} with ${context.self.path}")
        println(s"Started  this.context.self.path : ${this.context.self.path.name} with ${this.context.self.path}")
        this
    }

  }
}