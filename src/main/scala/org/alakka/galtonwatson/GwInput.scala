package org.alakka.galtonwatson
import org.montecarlo.Parameter
import org.montecarlo.Parameter.implicitConversions._


case class GwInput(
                    lambda          : Parameter[Double] = Vector(1.0,1.2,1.5,2.0),
                    maxPopulation   : Parameter[Long]   = 1000
                  )

