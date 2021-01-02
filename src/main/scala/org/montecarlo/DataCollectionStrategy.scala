package org.montecarlo


abstract class DataCollectionStrategy extends  Serializable {
  def collectData(trial:Trial):Boolean
}
