package test.vnode.hashing

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator
import com.datastax.oss.driver.api.core.context.DriverContext
import java.net.InetAddress
import java.net.InetSocketAddress

class WorkaroundAddressTranslator(
  @Suppress("UNUSED_PARAMETER") context: DriverContext
) : AddressTranslator {

  override fun translate(address: InetSocketAddress): InetSocketAddress {
    val port = with(address) { "${port.toString().take(3)}${hostString.takeLast(2)}" }.toInt()
    return InetSocketAddress(InetAddress.getLoopbackAddress(), port)
  }

  override fun close() {}
}
