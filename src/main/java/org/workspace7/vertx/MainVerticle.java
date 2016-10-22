package org.workspace7.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;

import java.util.List;
import java.util.stream.Collectors;

public class MainVerticle extends AbstractVerticle {

    private JDBCClient jdbcClient;

    @Override
    public void start(Future<Void> startFuture) {

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
            future.fail(http.cause());
        }
    }

    private void startBackend(Handler<AsyncResult<SQLConnection>> next, Future<Void> future) {
        jdbcClient.getConnection(ar -> {
            if (ar.failed()) {
                future.fail(ar.cause());
            } else {
                next.handle(Future.succeededFuture(ar.result()));
            }
        });
    }

    private void createChants(AsyncResult<SQLConnection> result, Handler<AsyncResult<Void>> next, Future<Void> future) {

        if (result.failed()) {
            future.fail(result.cause());
        } else {
            SQLConnection sqlConnection = result.result();
            sqlConnection.execute(
                    "CREATE TABLE IF NOT EXISTS MANTRAS (id INTEGER IDENTITY, mantra varchar(200))",
                    ar -> {
                        if (ar.failed()) {
                            System.err.println(ar.cause());
                            future.fail(ar.cause());
                            sqlConnection.close();
                            return;
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
                                                                        next.handle(Future.<Void>succeededFuture());
                                                                        sqlConnection.close();
                                                                    })));

                                        } else {
                                            next.handle(Future.<Void>succeededFuture());
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

        vertx.createHttpServer()
                .requestHandler(router::accept)
                .listen(
                        config().getInteger("http.port", 8080),
                        next::handle
                );

        //REST API

        router.get("/api/mantras").handler(this::getAll);
        //This is required
        router.route("/api/mantras*").handler(BodyHandler.create());
        router.post("/api/mantras").handler(this::addOne);
        router.get("/api/mantras/:id").handler(this::getOne);
        router.put("/api/mantras/:id").handler(this::updateOne);
        router.delete("/api/mantras/:id").handler(this::deleteOne);
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
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                        return;
                    }

                    ResultSet result = ar.result();
                    if (result.getNumRows() >= 1) {
                        resultHandler.handle(Future.succeededFuture(new Mantra(result.getRows().get(0))));
                    }
                }
        );
    }

    private void update(Mantra mantra, SQLConnection sqlConnection, Handler<AsyncResult<Mantra>> resultHandler) {
        final String sql = "UPDATE MANTRAS set mantra = ? where id=?";
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
                        return;
                    } else {
                        resultHandler.handle(Future.succeededFuture(mantra));
                    }
                }
        );
    }

    private void getOne(RoutingContext routingContext) {

        String id = routingContext.request().getParam("id");

        if (id != null) {
            jdbcClient.getConnection(ar -> {
                SQLConnection sqlConnection = ar.result();
                select(id, sqlConnection, (mantra) -> {
                    if (mantra != null) {
                        routingContext.response()
                                .putHeader("content-type", "application/json;charset=utf-8")
                                .end(Json.encodePrettily(mantra.result()));
                    } else {
                        routingContext.response().setStatusCode(404).end();
                    }
                });
            });
        } else {
            routingContext.response().setStatusCode(404).end();
        }
    }

    private void deleteOne(RoutingContext routingContext) {

        String id = routingContext.request().getParam("id");

        jdbcClient.getConnection(ar -> {

            SQLConnection sqlConnection = ar.result();

            if (id == null) {
                routingContext.response().setStatusCode(404).end();
            } else {
                sqlConnection.execute("DELETE FROM MANTRAS WHERE id='" + id + "'",
                        result -> {
                            routingContext.response()
                                    .putHeader("content-type", "text/plain")
                                    .setStatusCode(204).end();
                        });
            }

            sqlConnection.close();

        });
    }


    private void addOne(RoutingContext routingContext) {
        final Mantra mantra = Json.decodeValue(routingContext.getBodyAsString(), Mantra.class);
        jdbcClient.getConnection(ar -> {
            SQLConnection sqlConnection = ar.result();
            insert(mantra, sqlConnection, (inserted) -> {
                if (inserted.failed()) {
                    routingContext.response()
                            .setStatusCode(404)
                            .end();
                }
                routingContext.response()
                        .setStatusCode(201)
                        .putHeader("content-type", "application/json;charset=utf-8")
                        .end(Json.encodePrettily(inserted.result()));
                sqlConnection.close();
            });
        });
    }

    private void updateOne(RoutingContext routingContext) {
        final Mantra updatedMantra = Json.decodeValue(routingContext.getBodyAsString(), Mantra.class);

        jdbcClient.getConnection(ar -> {
            SQLConnection sqlConnection = ar.result();
            update(updatedMantra, sqlConnection,
                    (u) -> {
                        if (u.failed()) {
                            routingContext.response()
                                    .setStatusCode(404)
                                    .end();
                        } else {
                            routingContext.response()
                                    .putHeader("content-type", "application/json;charset=utf-8")
                                    .end(Json.encodePrettily(u.result()));
                        }

                    });
            sqlConnection.close();
        });
    }

    private void getAll(RoutingContext routingContext) {
        jdbcClient.getConnection(ar -> {
            SQLConnection sqlConnection = ar.result();
            sqlConnection.query("SELECT * FROM MANTRAS", result -> {
                List<Mantra> mantras = result.result().getRows().stream().map(Mantra::new).collect(Collectors.toList());
                routingContext.response()
                        .putHeader("content-type", "application/json;charset=utf-8")
                        .end(Json.encodePrettily(mantras));
            });
        });

    }


}