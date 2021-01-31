package org.montecarlo

import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}

object Implicits {

  /**
   * With this you can initialize parameters in a more intuitive way.
   * Input fields can get parameter definitions like
   * <ul>
   * <li>seedResourceAcquisitionFitness : Parameter[Double] = 1.2</li>
   * <li>seedResourceAcquisitionFitness : Parameter[Double] = Vector(1.2, 1.5, 2.0)</li>
   * <li>totalResource:Parameter[Long] = 1000 to 5000 by 1000</li>
   * <ul>
   */
  implicit def fromIndexedSeq[T](value: Seq[T]): Parameter[T] = Parameter[T](value)

  implicit def fromT[T](t: T): Parameter[T] = Parameter(t)

  implicit def toT[T](parameter: Parameter[T]): T = parameter.head()

  implicit def fromDataFrame(df: DataFrame): MvDataFrame = new MvDataFrame(df)
  implicit def fromMvDataFrame(mvDf: MvDataFrame): DataFrame = mvDf.df
  implicit def fromRelationalGroupedDataset(rgds: RelationalGroupedDataset): MvRelationalGroupedDataset
    = new MvRelationalGroupedDataset(rgds)
  implicit def fromMvRelationalGroupedDataset(mvRgds:MvRelationalGroupedDataset) :RelationalGroupedDataset
    = mvRgds.rgds

  implicit class StringToBigDecimal(val s: String) {
    def toBD() :BigDecimal = BigDecimal(s)
  }
}
