package com.opendigitaleducation.events.tasks;

import java.util.List;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

public abstract class AbstractETLTask implements Task {

    protected final PgPool masterPgPool;
    protected final PgPool slavePgPool;

    public AbstractETLTask(PgPool masterPgPool, PgPool slavePgPool) {
        this.masterPgPool = masterPgPool;
        this.slavePgPool = slavePgPool;
    }

    @Override
    public void execute(Handler<AsyncResult<Void>> handler) {
        extract(ar -> {
            if (ar.succeeded()) {
                transform(ar.result(), ar2 -> {
                    if (ar2.succeeded()) {
                        load(ar2.result(), handler);
                    } else {
                        handler.handle(Future.failedFuture(ar2.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    protected void extract(Handler<AsyncResult<RowSet<Row>>> handler) {
        slavePgPool.query(extractQuery()).execute(handler);
    }

    protected abstract void transform(RowSet<Row> rows, Handler<AsyncResult<List<Tuple>>> handler);

    protected void load(List<Tuple> tuples, Handler<AsyncResult<Void>> handler) {
        if (tuples.isEmpty()) {
            handler.handle(Future.succeededFuture());
        }
        masterPgPool.preparedQuery(loadQuery()).executeBatch(tuples, ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    protected abstract String extractQuery();

    protected abstract String loadQuery();

}
