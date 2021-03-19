#!/usr/bin/env bash

SEED_NODE=test_node_0

cqlsh() {
  docker-compose exec -e CQLSH_PORT=19042 "${SEED_NODE}" cqlsh -u cassandra -p cassandra "$@"
}

migrate() {
  cqlsh -e "$(<./migrations.cql)"
}

nodetool() {
  docker-compose exec "${SEED_NODE}" nodetool "$@"
}
