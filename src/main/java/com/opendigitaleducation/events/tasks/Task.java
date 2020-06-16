package com.opendigitaleducation.events.tasks;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface Task {

    void execute(Handler<AsyncResult<Void>> handler);

}
