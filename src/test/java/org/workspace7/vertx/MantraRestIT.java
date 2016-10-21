package org.workspace7.vertx;

import io.restassured.RestAssured;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.restassured.RestAssured.delete;
import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * @author kameshs
 */
public class MantraRestIT {

    @BeforeClass
    public static void configureRestAssured() {
        RestAssured.baseURI = "http://localhost";
        RestAssured.port = Integer.getInteger("http.port", 8080);
    }

    @AfterClass
    public static void deconfigureRestAssured() {
        RestAssured.reset();
    }

    @Test
    public void testGetMantraById() {
        //get all request and find one mantra
        final int id = get("/api/mantras").then()
                .assertThat().statusCode(200)
                .extract()
                .jsonPath().getInt("find { it.mantra = 'Srimathe Ramanujaya Namaha!'}.id");
        //use the id got to retrieve the mantra by id
        get("/api/mantras/" + id).then()
                .assertThat().statusCode(200)
                .body("mantra", equalTo("Srimathe Ramanujaya Namaha!"))
                .body("id", equalTo(id));
    }

    @Test
    public void testAddAndDelete() {
        Mantra mantra = given().body("{\"mantra\":\"Add and Delete me \"}")
                .request()
                .post("/api/mantras").thenReturn().as(Mantra.class);
        assertThat(mantra.getMantra()).isEqualToIgnoringCase("Add and Delete me ");
        assertThat(mantra.getId()).isNotZero();

        final int id = mantra.getId();

        //check if added
        get("/api/mantras/" + id).then()
                .assertThat().statusCode(200)
                .body("mantra", equalTo("Add and Delete me "))
                .body("id", equalTo(id));

        delete("/api/mantras/" + id).then().assertThat().statusCode(204);

        //The mantra should not exists
        get("/api/mantras/" + id).then()
                .assertThat().statusCode(404);

    }
}
