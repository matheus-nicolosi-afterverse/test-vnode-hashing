CREATE KEYSPACE IF NOT EXISTS test_keyspace_0
          WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE test_keyspace_0;

CREATE TABLE IF NOT EXISTS test_table_0
(
  key0       uuid,
  key1       uuid,
  value      int,
  created_at timeuuid,
  PRIMARY KEY (key0)
)
  WITH COMPACTION = {'class': 'LeveledCompactionStrategy'}
   AND COMPRESSION = {'enabled': 'false'};

CREATE TABLE IF NOT EXISTS test_table_1
(
  key0       uuid,
  key1       uuid,
  value      int,
  created_at timeuuid,
  PRIMARY KEY (key0, key1)
)
  WITH COMPACTION = {'class': 'LeveledCompactionStrategy'}
   AND COMPRESSION = {'enabled': 'false'};

CREATE TABLE IF NOT EXISTS test_table_2
(
  key0       uuid,
  key1       uuid,
  value      int,
  created_at timeuuid,
  PRIMARY KEY ((key0, key1))
)
  WITH COMPACTION = {'class': 'LeveledCompactionStrategy'}
   AND COMPRESSION = {'enabled': 'false'};
