package eu.thijslemmens.carbonpostgres.config

class CascadingConfigProvider(val providers: ConfigProvider*) extends ConfigProvider {
  /**
    * @param key The key of the parameter
    * @return The value of the parameter
    */
  override def getStringParameter(key: String): Option[String] = {
    providers collectFirst {
      case provider if(provider.getStringParameter(key).isDefined) => provider.getStringParameter(key).get
    }
  }
}
