package org.alakka
import akka.actor.typed.Behavior


object Experiment {
  def apply(trials:Int):Experiment = {
    new Experiment(trials)
  }


}

class Experiment(trials:Int) {
/*
  class MyTrial(override val param1:Int, ) extends Trial {
    override def postInit(): Unit = {
      super.postInit()
    }
  }
  class MySearchSpace {
    def Map[Int, ]
  }
  object MySearchSpace() {
    apply()
  }

  object MyTrial extends Trial {
    //def initContants(myTrial: MyTrial): Unit = {
     //
    //}

    def MyTrial apply(d1:Int, d2:Int) {
      // init constants
      val myTrial = new MyTrial()
      // init id
      myTrial.id = id
      // init dims
      myTrial.d11 = c1
    }
  }
      def initVariables(id:Int, dim1:Long, dim2:Double): Unit = {

        ....
      }
    }
    templateTrial.init(...)

    val dim1 = 0.1 to 0.9 by 0.01
    val iso = 1 to 100 by 2

    val trialRunResults = ids.map(id => SearchSpace(id))
      .map(searchVector => MyTrial(searchVector)).foreach(run)
    trialRunResults.reduce(result1 =>  result.x )
  }


*/

}

