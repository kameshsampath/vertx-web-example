package org.workspace7.vertx;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * @author kameshs
 */
@RunWith(VertxUnitRunner.class)
public class MainVerticleTest {

    private String apiHost = "localhost";
    private int port;
    private Vertx vertx;

    @Before
    public void setup(TestContext context) throws IOException {
        vertx = Vertx.vertx();
        //random port picking
        ServerSocket socket = new ServerSocket(0);
        port = socket.getLocalPort();
        socket.close();

        DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject()
                .put("http.port", port)
                .put("url", "jdbc:h2:mem:vdb")
                .put("user", "sa")
                .put("driver_class", "org.h2.Driver"));
        vertx.deployVerticle(MainVerticle.class.getName(), options, context.asyncAssertSuccess());
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testAssetsIndex(TestContext context) {
        Async async = context.async();

        vertx.createHttpClient().getNow(port, apiHost, "/assets/index.html", httpClientResponse -> {
            context.assertEquals(httpClientResponse.statusCode(), 200);
            context.assertEquals(httpClientResponse.getHeader("content-type"), "text/html;charset=UTF-8");
            httpClientResponse.bodyHandler(body -> {
                context.assertTrue(body.toString().contains("<h1>Hare Rama Hare Rama Rama Rama Hare Hare!</h1>"));
                async.complete();
            });
        });
    }

    @Test
    public void checkThatICanAdd(TestContext context) {
        Async async = context.async();
        final String chantJson = Json.encodePrettily(new Mantra("Hare Vittala!"));
        final String contentLength = Integer.toString(chantJson.length());
        vertx.createHttpClient().post(port, apiHost, "/api/mantras")
                .putHeader("content-type", "application/json")
                .putHeader("content-length", contentLength)
                .handler(httpClientResponse -> {
                    context.assertEquals(httpClientResponse.statusCode(), 201);
                    context.assertEquals(httpClientResponse.getHeader("content-type"), "application/json;charset=utf-8");
                    httpClientResponse.bodyHandler(body -> {
                        final Mantra mantra = Json.decodeValue(body.toString(), Mantra.class);
                        context.assertEquals(mantra.getMantra(), "Hare Vittala!");
                        context.assertNotNull(mantra.getId());
                        async.complete();
                    });
                })
                .write(chantJson)
                .end();
    }
}
