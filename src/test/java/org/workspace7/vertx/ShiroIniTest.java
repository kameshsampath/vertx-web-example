package org.workspace7.vertx;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.*;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.Factory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static junit.framework.TestCase.*;

/**
 * @author kameshs
 */
@RunWith(Parameterized.class)
public class ShiroIniTest {

    private UsernamePasswordToken usernamePasswordToken;
    private String userName;
    private String userPassword;
    private String expectedRole;

    public ShiroIniTest(String userName, String password, String role) {
        this.userName = userName;
        this.userPassword = password;
        this.expectedRole = role;
        System.out.println("Testing with user:" + userName + " and password:" + password + " with roles:" + role);
    }

    @BeforeClass
    public static void setup() {
        Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:test-shiro.ini");
        SecurityManager securityManager = factory.getInstance();
        assertNotNull(securityManager);
        SecurityUtils.setSecurityManager(securityManager);
    }

    @Parameterized.Parameters
    public static Collection users() {
        String[][] users = new String[][]{
                {"krishna", "krishna", "super-user"},
                {"guest", "guest", "guest"},
                {"unknown", "blah", "blah"},
                {"kamesh", "blah", "blah"}
        };

        return Arrays.asList(users);
    }

    @Before
    public void buildToken() {
        usernamePasswordToken = new UsernamePasswordToken(userName, userPassword);
    }

    @Test
    public void testAuthentication() {

        Subject subject = SecurityUtils.getSubject();

        if (!subject.isAuthenticated()) {
            try {
                subject.login(usernamePasswordToken);
                subject.logout();
            } catch (UnknownAccountException e) {
                if (!usernamePasswordToken.getUsername().equals("unknown")) {
                    fail("This is a valid account");
                }


            } catch (IncorrectCredentialsException e) {
                if (!usernamePasswordToken.getUsername().equals("kamesh")) {
                    fail("This is a valid account credentials");
                }

            } catch (LockedAccountException e) {
                fail("This is not a locked account");

            } catch (AuthenticationException e) {
                fail(e.getMessage());
            }
        }

    }

    @Test
    public void testAuthorization() {

        Subject subject = SecurityUtils.getSubject();

        if (!subject.isAuthenticated()) {
            try {
                subject.login(usernamePasswordToken);
                assertTrue(subject.hasRole(expectedRole));
            } catch (UnknownAccountException e) {
                if (!usernamePasswordToken.getUsername().equals("unknown")) {
                    fail("This is a valid account");
                }


            } catch (IncorrectCredentialsException e) {
                if (!usernamePasswordToken.getUsername().equals("kamesh")) {
                    fail("This is a valid account credentials");
                }

            } catch (LockedAccountException e) {
                fail("This is not a locked account");

            } catch (AuthenticationException e) {
                fail(e.getMessage());
            }
        }

    }

}
