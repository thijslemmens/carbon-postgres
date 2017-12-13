package eu.thijslemmens.carbonpostgres.config

trait ConfigProvider {

  /**
    * @param key The key of the parameter
    * @return The value of the parameter
    */
  def getStringParameter(key: String): Option[String]

  /**
    * @param key The key of the parameter
    * @return The value of the parameter
    */
  def getIntParameter(key: String): Option[Int] = {
    getStringParameter(key) match {
      case None => None
      case Some(s) => Some(s.toInt)
    }
  }

}
