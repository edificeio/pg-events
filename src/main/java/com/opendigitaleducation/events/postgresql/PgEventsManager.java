package com.opendigitaleducation.events.postgresql;

import java.time.LocalDateTime;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;

public class PgEventsManager {

    public enum RangeInterval { WEEK, MONTH }

    private static final Logger log = LoggerFactory.getLogger(PgEventsManager.class);
    private final PgPool masterPgPool;


    public PgEventsManager(PgPool mastPgPool) {
        this.masterPgPool = mastPgPool;
    }

    public void checkEventsTables(String schemasResource, Handler<AsyncResult<Void>> handler) {
        masterPgPool.query(
            "SELECT table_schema || '.' || table_name as table " +
            "FROM information_schema.tables " +
            "WHERE table_schema <> 'pg_catalog' and table_schema <> 'information_schema'")
        .execute(ar -> {
            if (ar.succeeded()) {
                final Set<String> existsTables = new HashSet<>();
                for (Row row: ar.result()) {
                    existsTables.add(row.getString("table"));
                }
                final JsonArray schemas = loadFromResource(schemasResource);
                if (!existsTables.containsAll(schemas.stream().map(t -> ((JsonObject)t).getString("table")).collect(Collectors.toList()))) {
                    masterPgPool.begin(res -> {
                        if (res.succeeded()) {
                            final Transaction tx = res.result();
                            final List<Future> f = new ArrayList<>();
                            for (Object o : schemas) {
                                createTableIfNotExists(existsTables, tx, (JsonObject) o).ifPresent(f::add);
                            }
                            CompositeFuture.all(f).onComplete(ar2 -> {
                                if (ar2.succeeded()) {
                                    tx.commit(handler);
                                } else {
                                    tx.rollback(handler);
                                }
                            });
                        } else {
                            handler.handle(Future.failedFuture(res.cause()));
                        }
                    });
                } else {
                    handler.handle(Future.succeededFuture());
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void addPartitionsTables(List<String> allowedSchemas, LocalDateTime startRange, LocalDateTime endRange, RangeInterval rangeInterval,
            Handler<AsyncResult<Void>> handler) {
        if (allowedSchemas == null || allowedSchemas.isEmpty()) {
            handler.handle(Future.failedFuture("Missing schema list"));
            return;
        }
        final Collector<Row, ?, List<PartitionTable>> collector = Collectors.mapping(
            row -> new PartitionTable(
                    row.getString("parent_schema") +"." + row.getString("parent_relname"),
                    row.getString("child_schema") + "." + row.getString("child_relname"),
                    row.getString("partition_expression")),
            Collectors.toList());
        final String query =
            "WITH events_tables as ( " +
                "SELECT nmsp_parent.nspname  AS parent_schema, parent.relname AS parent_relname, " +
                "nmsp_child.nspname  AS child_schema, child.relname AS child_relname, " +
                "pg_get_expr(child.relpartbound, child.oid, true) as partition_expression, " +
                "rank() OVER (PARTITION BY parent.relname ORDER BY child.relname DESC) as r " +
                "FROM pg_inherits " +
                "JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid " +
                "JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid " +
                "JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace " +
                "JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace " +
                "WHERE nmsp_parent.nspname IN " + IntStream
                .rangeClosed(1, allowedSchemas.size()).boxed().map(i -> "$" + i).collect(Collectors.joining(",", "(", ")")) +
            ") " +
            "SELECT parent_schema, parent_relname, child_schema, child_relname, partition_expression " +
            "FROM events_tables " +
            "where events_tables.r = 1 and partition_expression is not null " +
            "ORDER BY parent_relname ";
        final Tuple t = Tuple.tuple();
        allowedSchemas.stream().forEach(t::addString);
        masterPgPool.preparedQuery(query).collecting(collector).execute(t, ar -> {
            if (ar.succeeded()) {
                final List<PartitionTable> partitions = ar.result().value().stream()
                        .filter(x -> x.getDbEndRange().isBefore(endRange))
                        .collect(Collectors.toList());
                if (!partitions.isEmpty()) {
                    final boolean isWeekRange = RangeInterval.WEEK == rangeInterval;
                    masterPgPool.begin(res -> {
                        if (res.succeeded()) {
                            final Transaction tx = res.result();
                            final List<Future> f = new ArrayList<>();
                            for (PartitionTable partitionTable: partitions) {
                                LocalDateTime date;
                                if (startRange.isBefore(partitionTable.getDbEndRange())) {
                                    date = partitionTable.getDbEndRange();
                                } else {
                                    date = startRange;
                                }
                                if (isWeekRange) {
                                    final WeekFields weekFields = WeekFields.ISO;
                                    if (date.get(weekFields.dayOfWeek()) != 1) {
                                        final LocalDateTime nextDate = date.plusWeeks(1).with(weekFields.dayOfWeek(), 1);
                                        f.add(createPartitionOfTable(tx, partitionTable.getParentTableName(), date, true, nextDate));
                                        date = nextDate;
                                    }
                                } else {
                                    if (date.getDayOfMonth() != 1) {
                                        final LocalDateTime nextDate = date.plusMonths(1).withDayOfMonth(1);
                                        f.add(createPartitionOfTable(tx, partitionTable.getParentTableName(), date, false, nextDate));
                                        date = nextDate;
                                    }
                                }

                                while (date.isBefore(endRange)) {
                                    f.add(createPartitionOfTable(tx, partitionTable.getParentTableName(), date, isWeekRange, null));
                                    date = isWeekRange ? date.plusWeeks(1) : date.plusMonths(1);
                                }
                            }
                            CompositeFuture.all(f).onComplete(ar2 -> {
                                if (ar2.succeeded()) {
                                    tx.commit(handler);
                                } else {
                                    tx.rollback(handler);
                                }
                            });
                        } else {
                            handler.handle(Future.failedFuture(res.cause()));
                        }
                    });
                } else {
                    handler.handle(Future.succeededFuture());
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void dropOldEmptyPartitionsTables(List<String> allowedSchemas, LocalDateTime dropBeforeDate, Handler<AsyncResult<Void>> handler) {
        if (allowedSchemas == null || allowedSchemas.isEmpty()) {
            handler.handle(Future.failedFuture("Missing schema list"));
            return;
        }

        final Collector<Row, ?, List<PartitionTable>> collector = Collectors.mapping(
            row -> new PartitionTable(null,
                    row.getString("child_schema") + "." + row.getString("child_relname"),
                    row.getString("partition_expression")),
            Collectors.toList());
        final String query =
                "SELECT nmsp_child.nspname AS child_schema, child.relname AS child_relname, " +
                "pg_get_expr(child.relpartbound, child.oid, true) as partition_expression " +
                "FROM pg_inherits " +
                "JOIN pg_class child ON pg_inherits.inhrelid = child.oid " +
                "JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace " +
                "WHERE nmsp_child.nspname IN " + IntStream
                        .rangeClosed(1, allowedSchemas.size()).boxed().map(i -> "$" + i).collect(Collectors.joining(",", "(", ")")) +
                " and child.reltuples = 0 and pg_get_expr(child.relpartbound, child.oid, true) is not null ";
        final Tuple t = Tuple.tuple();
        allowedSchemas.stream().forEach(t::addString);
        masterPgPool.preparedQuery(query).collecting(collector).execute(t, ar -> {
            if (ar.succeeded()) {
                final List<PartitionTable> partitions = ar.result().value().stream()
                        .filter(x -> x.getDbEndRange().isBefore(dropBeforeDate))
                        .collect(Collectors.toList());
                if (!partitions.isEmpty()) {
                    masterPgPool.begin(res -> {
                        if (res.succeeded()) {
                            final Transaction tx = res.result();
                            final List<Future> f = new ArrayList<>();
                            partitions.forEach(p -> f.add(p.removePartition(masterPgPool, tx)));
                            CompositeFuture.all(f).onComplete(ar2 -> {
                                if (ar2.succeeded()) {
                                    tx.commit(handler);
                                } else {
                                    tx.rollback(handler);
                                }
                            });
                        } else {
                            handler.handle(Future.failedFuture(res.cause()));
                        }
                    });
                } else {
                    handler.handle(Future.succeededFuture());
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private Optional<Future<Void>> createTableIfNotExists(Set<String> existsTables, Transaction tx, JsonObject schema) {
        Promise<Void> promise = Promise.promise();
        final String tableName = schema.getString("table");
        if (!existsTables.contains(tableName)) {
            final StringBuilder sb = new StringBuilder();
            if (tableName.endsWith("_events")) {
                sb.append("CREATE TABLE ").append(tableName).append(" ( id UUID NOT NULL, date TIMESTAMP NOT NULL");
                for (Object o: schema.getJsonArray("attributes")) {
                    JsonObject attribute = (JsonObject) o;
                    for (String attr: attribute.fieldNames()) {
                        sb.append(", ").append(attr).append(" ").append(attribute.getString(attr));
                    }
                }
                sb.append(") PARTITION BY RANGE (date); ");
                tx.query(sb.toString()).execute(ar -> {
                    if (ar.succeeded()) {
                        LocalDateTime date = LocalDateTime.now().withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
                        final int year = date.getYear();
                        List<Future> f = new ArrayList<>();
                        while (date.getMonthValue() != 9 || date.getYear() <= year) {
                            f.add(createPartitionOfTable(tx, tableName, date));
                            date = date.plusMonths(1);
                        }
                        CompositeFuture.all(f).onComplete(ar2 -> {
                            if (ar2.succeeded()) {
                                promise.complete();
                            } else {
                                promise.fail(ar.cause());
                            }
                        });
                    } else {
                        log.error("Error creating table partition : " + tableName, ar.cause());
                        promise.fail(ar.cause());
                    }
                });
            } else {
                sb.append("CREATE TABLE ").append(tableName).append(" (");
                for (Object o: schema.getJsonArray("attributes")) {
                    JsonObject attribute = (JsonObject) o;
                    for (String attr: attribute.fieldNames()) {
                        sb.append(attr).append(" ").append(attribute.getString(attr)).append(",");
                    }
                }
                sb.deleteCharAt(sb.length() -1).append(")");
                tx.query(sb.toString()).execute(ar -> {
                    if (ar.succeeded()) {
                        promise.complete();
                    } else {
                        log.error("Error creating table : " + tableName, ar.cause());
                        promise.fail(ar.cause());
                    }
                });
            }
            return Optional.of(promise.future());
        } else {
            return Optional.empty();
        }
    }

    private Future<Void> createPartitionOfTable(Transaction tx, String tableName, LocalDateTime date) {
        return createPartitionOfTable(tx, tableName, date, false, null);
    }

    private Future<Void> createPartitionOfTable(Transaction tx, String tableName, LocalDateTime date, boolean partitionByWeek, LocalDateTime endDateTime) {
        final Promise<Void> promise = Promise.promise();
        final StringBuilder sb = new StringBuilder();
        final String subtableName;
        final LocalDateTime excludeLimitDateTime;
        if (partitionByWeek) {
            WeekFields weekFields = WeekFields.ISO;
            int year = date.getYear();
            if (date.getMonthValue() == 12 && date.get(weekFields.weekOfWeekBasedYear()) == 1) {
                year++;
            }
            subtableName = tableName + year + String.format("%1$2s", date.get(weekFields.weekOfWeekBasedYear())).replace(' ', '0');
            excludeLimitDateTime = endDateTime != null ? endDateTime : date.plusWeeks(1);
        } else {
            subtableName = tableName + date.getYear() + String.format("%1$2s", date.getMonthValue()).replace(' ', '0');
            excludeLimitDateTime = endDateTime != null ? endDateTime : date.plusMonths(1);
        }
        sb.append("CREATE TABLE ").append(subtableName)
                .append(" PARTITION OF ").append(tableName).append(" FOR VALUES FROM ('").append(date.toString())
                .append("') TO ('").append(excludeLimitDateTime.toString()).append("');");
        tx.query(sb.toString()).execute(ar -> {
            if (ar.succeeded()) {
                final StringBuilder sb2 = new StringBuilder();
                sb2.append("CREATE INDEX ON ").append(subtableName).append(" USING brin (date);");
                tx.query(sb2.toString()).execute(ar2 -> {
                    if (ar2.succeeded()) {
                        promise.complete();
                    } else {
                        log.error("Error creating index : " + subtableName, ar2.cause());
                        promise.fail(ar2.cause());
                    }
                });
            } else {
                log.error("Error creating sub partition table : " + subtableName, ar.cause());
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    public static JsonArray loadFromResource(String resource) {
		final String src = new Scanner(PgEventsManager.class.getClassLoader()
				.getResourceAsStream(resource), "UTF-8")
				.useDelimiter("\\A").next();
		return new JsonArray(src);
    }

}
