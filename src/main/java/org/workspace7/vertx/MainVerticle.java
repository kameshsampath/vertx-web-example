package org.workspace7.vertx;

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.hawkular.apm.client.opentracing.APMTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.workspace7.vertx.tracing.HttpHeadersExtractAdapter;
import org.workspace7.vertx.tracing.VertxMessageInjectAdapter;

import java.util.List;
import java.util.stream.Collectors;

public class MainVerticle extends AbstractVerticle {

    private static final Logger _logger = LoggerFactory.getLogger(MainVerticle.class);
    private JDBCClient jdbcClient;
    private Tracer tracer = new APMTracer();

    @Override
    public void start(Future<Void> startFuture) {

        JsonObject appConfig = config();

        _logger.debug("Config: {}", appConfig);

        jdbcClient = JDBCClient.createShared(vertx, config(), "My-Mantras-Collection");

        startBackend(
                (connection) -> createChants(connection,
                        (nothing) -> startWebApp(
                                (http) -> completeStartup(http, startFuture)
                        ), startFuture
                ), startFuture);

    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        jdbcClient.close();
        stopFuture.complete();
    }

    private void completeStartup(AsyncResult<HttpServer> http, Future<Void> future) {
        if (http.succeeded()) {
            future.complete();
        } else {
            _logger.error("Error starting Verticle ", http.cause());
            future.fail(http.cause());
        }
    }

    private void startBackend(Handler<AsyncResult<SQLConnection>> next, Future<Void> future) {
        jdbcClient.getConnection(ar -> {
            if (ar.failed()) {
                _logger.error("Error starting backend ", ar.cause());
                future.fail(ar.cause());
            } else {
                next.handle(Future.succeededFuture(ar.result()));
            }
        });
    }

    private void createChants(AsyncResult<SQLConnection> result, Handler<AsyncResult<Void>> next, Future<Void> future) {

        if (result.failed()) {
            _logger.error("Error before creating chants ", result.cause());
            future.fail(result.cause());
        } else {
            SQLConnection sqlConnection = result.result();
            sqlConnection.execute(
                    "CREATE TABLE IF NOT EXISTS MANTRAS (id INTEGER IDENTITY, mantra varchar(200))",
                    ar -> {
                        if (ar.failed()) {
                            _logger.error("Error before creating chants  tables", ar.cause());
                            future.fail(ar.cause());
                            sqlConnection.close();
                        } else {
                            sqlConnection.query("SELECT * FROM MANTRAS",
                                    select -> {
                                        if (select.failed()) {
                                            future.fail(select.cause());
                                            sqlConnection.close();
                                            return;
                                        }
                                        if (select.result().getNumRows() == 0) {
                                            insert(new Mantra("Srimathe Ramanujaya Namaha!"), sqlConnection,
                                                    (m1) -> insert(new Mantra("Hare Rama Hare Rama Rama Rama Hare Hare!"), sqlConnection,
                                                            (m2) -> insert(new Mantra("Hare Krishna Hare Krishna Krishna Krishna Hare Hare!"), sqlConnection,
                                                                    (s) -> {
                                                                        next.handle(Future.succeededFuture());
                                                                        sqlConnection.close();
                                                                    })));

                                        } else {
                                            next.handle(Future.succeededFuture());
                                            sqlConnection.close();
                                        }
                                    });
                        }

                    }
            );

        }
    }

    private void startWebApp(Handler<AsyncResult<HttpServer>> next) {

        Router router = Router.router(vertx);

        //routes
        router.route("/").handler(routingContext -> {

            HttpServerResponse response = routingContext.response();
            response
                    .putHeader("content-type", "text/html")
                    .end("<h1 style=\"color:green\">Hare Rama Hare Rama Rama Rama Hare Hare!</h1><br/>" +
                            "<h2 style=\"color:blue\">Hare Krishna Hare Krishna Krishna Krishna Hare Hare!</h2>");
        });

        //static routes
        router.route("/assets/*").handler(StaticHandler.create("assets"));


//        //Authentication and Authorization
//        BasicAuthHandler basicAuthHandler = BasicAuthHandler.create(authProvider);
//
//        router.route("/admin/*").handler(basicAuthHandler);

        //REST API


        router.get("/api/mantras").handler(this::getAll);

        //This is required
        router.route("/api/mantras*").handler(BodyHandler.create());
        router.post("/api/mantras").handler(this::addOne);
        router.get("/api/mantras/:id").handler(this::getOne);
        router.put("/api/mantras/:id").handler(this::updateOne);
        router.delete("/api/mantras/:id").handler(this::deleteOne);

        vertx
                .createHttpServer()
                .requestHandler(router::accept)
                .listen(config().getInteger("http.port", 8080), next);

    }

    //CRUD Operations
    private void insert(Mantra mantra, SQLConnection sqlConnection, Handler<AsyncResult<Mantra>> next) {
        final String sql = "INSERT INTO MANTRAS(mantra) VALUES(?)";
        sqlConnection.updateWithParams(sql,
                new JsonArray().add(mantra.getMantra()),
                (ar) -> {
                    if (ar.failed()) {
                        next.handle(Future.failedFuture(ar.cause()));
                        sqlConnection.close();
                        return;
                    }

                    UpdateResult result = ar.result();
                    Mantra mantra1 = new Mantra(result.getKeys().getInteger(0), mantra.getMantra());
                    next.handle(Future.succeededFuture(mantra1));
                }
        );
    }

    private void select(String id, SQLConnection sqlConnection, Handler<AsyncResult<Mantra>> resultHandler) {
        final String sql = "SELECT * FROM MANTRAS where id=?";
        sqlConnection.queryWithParams(sql,
                new JsonArray().add(id),
                (ar) -> {
                    if (ar.failed()) {
                        resultHandler.handle(Future.failedFuture("Mantra Not Found"));
                    } else {
                        ResultSet result = ar.result();
                        if (result.getNumRows() >= 1) {
                            resultHandler.handle(Future.succeededFuture(new Mantra(result.getRows().get(0))));
                        } else {
                            resultHandler.handle(Future.failedFuture("Mantra Not Found"));
                        }
                    }
                }
        );
    }

    private void update(Mantra mantra, SQLConnection sqlConnection, Handler<AsyncResult<Mantra>> resultHandler) {
        final String sql = "UPDATE MANTRAS set MANTRA = ? where ID=?";
        sqlConnection.updateWithParams(sql,
                new JsonArray().add(mantra.getMantra()).add(mantra.getId()),
                update -> {
                    if (update.failed()) {
                        resultHandler.handle(Future.failedFuture("Unable to update mantra"));
                        return;
                    }

                    UpdateResult result = update.result();
                    if (result.getUpdated() == 0) {
                        resultHandler.handle(Future.failedFuture("Mantra not found"));
                    } else {
                        resultHandler.handle(Future.succeededFuture(mantra));
                    }
                }
        );
    }

    private void getOne(RoutingContext routingContext) {


        String id = routingContext.request().getParam("id");

        SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP,
                new HttpHeadersExtractAdapter(routingContext.request().headers()));

        Span getOneSpan = tracer.buildSpan("GET")
                .asChildOf(spanContext)
                .withTag("http.url", "/api/mantras")
                .withTag("service", "ChantManager")
                .withTag("transaction", "Get a Mantra")
                .start();

        Span mantrasFromDB = tracer.buildSpan("MantrasFromDB")
                .asChildOf(getOneSpan)
                .start();

        tracer.inject(mantrasFromDB.context(), Format.Builtin.TEXT_MAP,
                new VertxMessageInjectAdapter(new JsonObject().put("id", id)));


        if (id != null) {
            jdbcClient.getConnection(ar -> {
                SQLConnection sqlConnection = ar.result();
                select(id, sqlConnection, (mantra) -> {
                    mantrasFromDB.finish();
                    if (mantra.succeeded()) {
                        routingContext.response()
                                .putHeader("content-type", "application/json;charset=utf-8")
                                .end(Json.encodePrettily(mantra.result()));
                        getOneSpan.finish();
                    } else {
                        sendError(404, "Unable to get mantra with id:" + id,
                                routingContext.response(), getOneSpan);
                    }
                });
            });
        } else {
            routingContext.response().setStatusCode(404).end();
        }
    }

    private void deleteOne(RoutingContext routingContext) {

        String id = routingContext.request().getParam("id");

        SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP,
                new HttpHeadersExtractAdapter(routingContext.request().headers()));

        Span deleteMantra = tracer.buildSpan("DELETE")
                .asChildOf(spanContext)
                .withTag("http.url", "/api/mantras")
                .withTag("service", "ChantManager")
                .withTag("transaction", "Delete Mantra")
                .start();

        Span addMantraToDB = tracer.buildSpan("DeleteMantraFromDB")
                .asChildOf(deleteMantra)
                .start();

        tracer.inject(addMantraToDB.context(), Format.Builtin.TEXT_MAP,
                new VertxMessageInjectAdapter(new JsonObject().put("id", id)));

        jdbcClient.getConnection(ar -> {

            SQLConnection sqlConnection = ar.result();

            if (id == null) {
                addMantraToDB.finish();
                sendError(404, "Unable to delete mantra with id:" + id,
                        routingContext.response(), deleteMantra);
            } else {
                sqlConnection.execute("DELETE FROM MANTRAS WHERE id='" + id + "'",
                        (result) -> {

                            if (result.succeeded()) {
                                routingContext.response()
                                        .putHeader("content-type", "text/plain")
                                        .setStatusCode(204).end();
                            } else {
                                sendError(404, result.cause().getMessage(), routingContext.response(), deleteMantra);
                            }
                        });
                addMantraToDB.finish();
            }

            sqlConnection.close();

        });
    }


    private void addOne(RoutingContext routingContext) {

        SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP,
                new HttpHeadersExtractAdapter(routingContext.request().headers()));

        Span addNewMantraSpan = tracer.buildSpan("POST")
                .asChildOf(spanContext)
                .withTag("http.url", "/api/mantras")
                .withTag("service", "ChantManager")
                .withTag("transaction", "Add Mantra")
                .start();

        Span addMantraToDB = tracer.buildSpan("AddMantraToDB")
                .asChildOf(addNewMantraSpan)
                .start();

        tracer.inject(addMantraToDB.context(), Format.Builtin.TEXT_MAP,
                new VertxMessageInjectAdapter(routingContext.getBodyAsJson()));

        final Mantra mantra = Json.decodeValue(routingContext.getBodyAsString(), Mantra.class);
        jdbcClient.getConnection(ar -> {
            SQLConnection sqlConnection = ar.result();
            insert(mantra, sqlConnection, (inserted) -> {
                addMantraToDB.finish();

                if (inserted.failed()) {
                    sendError(404, "Unable to add mantra",
                            routingContext.response(), addNewMantraSpan);
                } else {

                    routingContext.response()
                            .setStatusCode(201)
                            .putHeader("content-type", "application/json;charset=utf-8")
                            .end(Json.encodePrettily(inserted.result()));
                    addNewMantraSpan.finish();
                }
                sqlConnection.close();
            });
        });
    }

    private void updateOne(RoutingContext routingContext) {
        final Mantra updatedMantra = Json.decodeValue(routingContext.getBodyAsString(), Mantra.class);

        SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP,
                new HttpHeadersExtractAdapter(routingContext.request().headers()));

        Span updateSpan = tracer.buildSpan("PUT")
                .asChildOf(spanContext)
                .withTag("http.url", "/api/mantras")
                .withTag("service", "ChantManager")
                .withTag("transaction", "Update Mantra")
                .start();

        Span mantrasFromDB = tracer.buildSpan("MantrasFromDB")
                .asChildOf(updateSpan)
                .start();

        tracer.inject(mantrasFromDB.context(), Format.Builtin.TEXT_MAP,
                new VertxMessageInjectAdapter(routingContext.getBodyAsJson()));

        jdbcClient.getConnection(ar -> {
            SQLConnection sqlConnection = ar.result();
            update(updatedMantra, sqlConnection,
                    (u) -> {
                        mantrasFromDB.finish();
                        if (u.failed()) {
                            sendError(404, u.cause().getMessage(), routingContext.response(), updateSpan);
                        } else {
                            routingContext.response()
                                    .putHeader("content-type", "application/json;charset=utf-8")
                                    .end(Json.encodePrettily(u.result()));
                            updateSpan.finish();
                        }

                    });
            sqlConnection.close();
        });
    }

    private void getAll(RoutingContext routingContext) {

        SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP,
                new HttpHeadersExtractAdapter(routingContext.request().headers()));

        Span getAllSpan = tracer.buildSpan("GET")
                .asChildOf(spanContext)
                .withTag("http.url", "/api/mantras")
                .withTag("service", "ChantManager")
                .withTag("transaction", "List all Mantras")
                .start();

        Span getMantrasFromDBSpan = tracer.buildSpan("MantrasFromDB")
                .asChildOf(getAllSpan)
                .start();

        tracer.inject(getMantrasFromDBSpan.context(), Format.Builtin.TEXT_MAP,
                new VertxMessageInjectAdapter(new JsonObject()));

        jdbcClient.getConnection(ar -> {
            SQLConnection sqlConnection = ar.result();
            sqlConnection.query("SELECT * FROM MANTRAS", result -> {

                getMantrasFromDBSpan.finish();

                if (result.succeeded()) {
                    List<Mantra> mantras = result.result().getRows().stream().map(Mantra::new).collect(Collectors.toList());
                    routingContext.response()
                            .putHeader("content-type", "application/json;charset=utf-8")
                            .end(Json.encodePrettily(mantras));
                    getAllSpan.finish();
                } else {
                    sendError(500, result.cause().getMessage(), routingContext.response(), getAllSpan);
                }
                sqlConnection.close();
            });
        });
    }


    private void sendError(int statusCode, String message, HttpServerResponse response, Span span) {
        response.setStatusCode(statusCode).end(message);
        if (span != null) {
            span.setTag("fault", message == null ? Integer.toString(statusCode) : message);
            span.finish();
        }
    }
}