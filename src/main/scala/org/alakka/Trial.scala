package org.alakka

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}



object Trial {
  sealed trait Command
  case object Start extends Command
  //final case class GetValue(replyTo: ActorRef[Value]) extends Command
  //final case class Value(n: Int)

  def apply(): Behavior[Command] = {
    Behaviors.setup(context => new Trial(context))
  }
}
class Trial (context: ActorContext[Trial.Command]) extends AbstractBehavior[Trial.Command](context) {

  import Trial._

  private var n = 0

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case Trial.Start =>
        context.log.debug("Trial Started")
        this

    }
  }
}

