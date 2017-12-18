package eu.thijslemmens.carbonpostgres.config

class EnvConfigProvider extends ConfigProvider {

  /**
    * @param key The key of the parameter
    * @return The value of the parameter
    */
  override def getStringParameter(key: String): Option[String] = {
    System.getenv(key) match {
      case null => None
      case s => Some(s)
    }
  }
}
