package org.alakka
// Entry point for creating all kind of Monte Carlo-based Artifical Life simpulations

import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}

object ExperimentController {
  trait Message
  trait Command extends Message
  trait Response extends Message
  trait Broadcast extends Message

  case class StartCommand(val trials:Int=100) extends Command
  //case class ExperimentControlStatus() extends Response with Experiment.TrialResponseAdapter
  def apply(): Behavior[Message] = {
    Behaviors.setup(context => new ExperimentController(context))

  }
  def main(args:Array[String]) :Unit = {
    val experimentController: ActorSystem[ExperimentController.Message]
    = ActorSystem(ExperimentController(), name="alakka")

    experimentController ! ExperimentController.StartCommand(trials=100)

    //experiment.terminate()

  }

}
class ExperimentController(context: ActorContext[ExperimentController.Message])
  extends AbstractBehavior[ExperimentController.Message](/*context*/) {

  override def onMessage(msg: ExperimentController.Message): Behavior[ExperimentController.Message] = {
    msg match {
      case ExperimentController.StartCommand(trials) =>
        //val experiment = context.spawn(Experiment(), "galton-watson-exp")
        //experiment ! Experiment.StartCommand(trials)

        //context.log.debug(s"Started ${experiment.path.name} with ${experiment.path}")


        //println(s"Started  context.self.path : ${context.self.path.name} with ${context.self.path}")
        //println(s"Started  this.context.self.path : ${this.context.self.path.name} with ${this.context.self.path}")
        this
    }

  }
}