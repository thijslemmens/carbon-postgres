package eu.thijslemmens.carbonpostgres.config

class DefaultConfigProvider extends ConfigProvider {

  private val config: Map[String, String] = Map(
    "db.host" -> "localhost",
    "db.port" -> "3456",
    "db.user" -> "timescale",
    "db.password" -> "timescale",
    "db.poolsize" -> "10",
    "db.database" -> "timescale",
    "carbon.tcp.host" -> "0.0.0.0",
    "carbon.tcp.port" -> "2003"
  )

  /**
    * @param key The key of the parameter
    * @return The value of the parameter
    */
  override def getStringParameter(key: String): Option[String] = {
    config.get(key)
  }
}
