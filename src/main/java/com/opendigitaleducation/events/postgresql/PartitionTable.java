package com.opendigitaleducation.events.postgresql;

import java.time.LocalDateTime;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Transaction;

class PartitionTable {

    private static final Logger log = LoggerFactory.getLogger(PartitionTable.class);

    private final String parentTableName;
    private final String tableName;
    private final LocalDateTime dbEndRange;
    private Integer nbRows = null;

    public PartitionTable(String parentTableName, String tableName, String partitionExpression) {
        this.parentTableName = parentTableName;
        this.tableName = tableName;
        this.dbEndRange = endDateFromPartitionExpression(partitionExpression);
    }

    public LocalDateTime getDbEndRange() {
        return dbEndRange;
    }

    public String getTableName() {
        return tableName;
    }

    public String getParentTableName() {
        return parentTableName;
    }

    public Future<Void> removePartition(PgPool pgPool, Transaction tx) {
        final Promise<Void> promise = Promise.promise();
        final Handler<AsyncResult<Void>> handler = removeAr -> {
            if (removeAr.succeeded()) {
                promise.complete();
            } else {
                promise.fail(removeAr.cause());
            }
        };
        if (nbRows == null) {
            countRows(pgPool, ar -> {
                if (ar.succeeded()) {
                    removePartition(tx, handler);
                } else {
                    promise.fail(ar.cause());
                }
            });
        } else {
            removePartition(tx, handler);
        }
        return promise.future();
    }

    private void removePartition(Transaction tx, Handler<AsyncResult<Void>> handler) {
        if (nbRows == null || nbRows != 0) {
            log.warn("Ignore dropping " + tableName + " because table isn't empty.");
            handler.handle(Future.succeededFuture());
            return;
        }

        final String query = "DROP TABLE " + tableName;
        tx.query(query).execute(ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void countRows(PgPool pgPool, Handler<AsyncResult<Integer>> handler) {
        final String query = "SELECT COUNT(*) as nb_rows FROM " + tableName;
        pgPool.query(query).execute(ar -> {
            if (ar.succeeded()) {
                Integer r = null;
                for (Row row: ar.result()) {
                    r = row.getInteger("nb_rows");
                }
                nbRows = r;
                handler.handle(Future.succeededFuture(r));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public static LocalDateTime endDateFromPartitionExpression(String partitionExpression) {
        return LocalDateTime.parse(partitionExpression.split("'\\)\\s*TO\\s*\\('")[1].replace("')", "").replace(" ", "T"));
    }

}
