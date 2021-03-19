package test.vnode.hashing

import com.datastax.oss.driver.api.core.CqlSession
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory
import java.util.*

@ExperimentalCoroutinesApi
suspend fun main(args: Array<String>) {
  val hostId = UUID.fromString(args[0])

  logger.info("Target node: $hostId")

  CqlSession.builder().build().use { cqlSession ->
    val node = cqlSession.getNode(hostId)
    val tokenRanges = cqlSession.getTokenRanges(node)

    flow {
      repeat(1_000_000) {
        val key0 = UUID.randomUUID()
        val key1 = UUID.randomUUID()
        val token0 = cqlSession.newToken(key0)
        val token1 = cqlSession.newToken(key0, key1)

        if (tokenRanges.any { it.contains(token0) }) {
          if (tokenRanges.any { it.contains(token1) }) {
            logger.debug("Found composite key ($key0, $key1) (token $token1)")

            emit(CompositeKey(key0, key1))
          } else {
            logger.debug("Found simple key ($key0) (token $token0)")

            emit(SimpleKey(key0))
          }
        } else {
          logger.debug("Discarded generated keys [key0=$key0, key1=$key1]")
        }
      }
    }
      .buffer(32)
      .onEach {
        logger.debug("Using key $it")

        when (it) {
          is CompositeKey -> {
            cqlSession.insertIntoTable0(it.key0).await()
            cqlSession.insertIntoTable1(it.key0).await()
            cqlSession.insertIntoTable2(it.key0, it.key1).await()
          }
          is SimpleKey -> {
            cqlSession.insertIntoTable0(it.key0).await()
            cqlSession.insertIntoTable1(it.key0).await()
          }
        }
      }
      .flowOn(Dispatchers.IO)
      .collect()
  }
}

private val logger = LoggerFactory.getLogger("main")

sealed class PartitionKey

data class CompositeKey(val key0: UUID, val key1: UUID) : PartitionKey()

data class SimpleKey(val key0: UUID) : PartitionKey()
