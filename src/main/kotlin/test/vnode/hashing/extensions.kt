package test.vnode.hashing

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.metadata.Node
import com.datastax.oss.driver.api.core.uuid.Uuids
import java.nio.ByteBuffer
import java.util.*

internal fun CqlSession.getNode(hostId: UUID) =
  metadata.nodes[hostId] ?: throw IllegalArgumentException("Node not found")

internal fun CqlSession.getTokenRanges(node: Node) =
  metadata.tokenMap
    .map { it.getTokenRanges(node) }
    .orElseThrow { IllegalStateException("Node token ranges not found") }
    .map { it.start..it.end }

internal fun CqlSession.newToken(vararg partitionKey: UUID) =
  metadata.tokenMap
    .map { it.newToken(*partitionKey.toByteBufferArray()) }
    .orElseThrow { IllegalStateException("Error creating new token") }

internal fun Array<out UUID>.toByteBufferArray() = map { it.toByteBuffer() }.toTypedArray()

internal fun UUID.toByteBuffer() = ByteBuffer.wrap(toString().toByteArray())

internal const val INSERT_INTO_TABLE_TEMPLATE =
  "INSERT INTO %s (key0, key1, value, created_at) VALUES (:key0, :key1, :value, :created_at)"

internal val CqlSession.insertIntoTable0 get() = prepare(INSERT_INTO_TABLE_TEMPLATE.format("test_table_0"))

internal fun CqlSession.insertIntoTable0(key0: UUID) =
  executeAsync(insertIntoTable0.bind(key0, UUID.randomUUID(), (0..9).random(), Uuids.timeBased()))

internal val CqlSession.insertIntoTable1 get() = prepare(INSERT_INTO_TABLE_TEMPLATE.format("test_table_1"))

internal fun CqlSession.insertIntoTable1(key0: UUID) =
  executeAsync(insertIntoTable1.bind(key0, UUID.randomUUID(), (0..9).random(), Uuids.timeBased()))

internal val CqlSession.insertIntoTable2 get() = prepare(INSERT_INTO_TABLE_TEMPLATE.format("test_table_2"))

internal fun CqlSession.insertIntoTable2(key0: UUID, key1: UUID) =
  executeAsync(insertIntoTable2.bind(key0, key1, (0..9).random(), Uuids.timeBased()))
