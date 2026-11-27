package zio

package object macros {

  /**
   * Adds `reloadable` to a layer that builds a service from no inputs.
   *
   * @see [[https://zio.dev/reference/service-pattern/reloadable-services/ Reloadable Services]]
   */
  implicit final class ReloadableSyntax[Service](private val layer: ZLayer[Any, Any, Service]) extends AnyVal {

    /**
     * Returns a layer that constructs a version of the service output by this
     * layer that can be dynamically reloaded with `ServiceReloader.reload`.
     */
    def reloadable(implicit
      tag: Tag[Service],
      isReloadable: IsReloadable[Service],
      trace: Trace
    ): ZLayer[ServiceReloader, ServiceReloader.Error, Service] =
      ZLayer.fromZIO(ServiceReloader.register(layer))
  }
}
