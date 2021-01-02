package org.montecarlo

import org.apache.spark.sql.{Encoder, Encoders}


class ComprehensiveDataCollection extends DataCollectionStrategy  with Serializable  {

  override def collectData(trial: Trial): Boolean = true
}


object ComprehensiveDataCollection {
  implicit val comprehensiveDataCollection: Encoder[ComprehensiveDataCollection]
    = Encoders.kryo[ComprehensiveDataCollection]

}
