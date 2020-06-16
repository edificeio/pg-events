package com.opendigitaleducation.events.postgresql;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

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

public class PgEventsManager {

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
                                f.add(createTableIfNotExists(existsTables, tx, (JsonObject) o));
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

    private Future<Void> createTableIfNotExists(Set<String> existsTables, Transaction tx, JsonObject schema) {
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
        }
        return promise.future();
    }

    private Future<Void> createPartitionOfTable(Transaction tx, String tableName, LocalDateTime date) {
        final Promise<Void> promise = Promise.promise();
        final StringBuilder sb = new StringBuilder();
        final String subtableName = tableName + date.getYear() + String.format("%1$2s", date.getMonthValue()).replace(' ', '0');
        sb.append("CREATE TABLE ").append(subtableName)
                .append(" PARTITION OF ").append(tableName).append(" FOR VALUES FROM ('").append(date.toString())
                .append("') TO ('").append(date.plusMonths(1).toString()).append("');");
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
