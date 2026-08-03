package vn.dth.khaithac.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.oauth2.client.oidc.web.logout.OidcClientInitiatedLogoutSuccessHandler;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.web.SecurityFilterChain;

import static org.springframework.security.config.Customizer.withDefaults;

/**
 * Hai đường vào, cùng một realm Keycloak:
 *  - Trình duyệt: oauth2Login (BFF) — session cookie HttpOnly, browser không giữ token.
 *  - Agent/MCP:  Bearer JWT (resource server).
 * /api/** luôn đòi xác thực — fail closed.
 * Logout = RP-initiated: hủy session local RỒI hủy luôn phiên SSO trên Keycloak
 * (không thì bấm đăng nhập lại là tự vào lại user cũ, không đổi được tài khoản).
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http, ClientRegistrationRepository clients) throws Exception {
        var oidcLogout = new OidcClientInitiatedLogoutSuccessHandler(clients);
        oidcLogout.setPostLogoutRedirectUri("{baseUrl}");

        http
                .authorizeHttpRequests(a -> a
                        // Trang + bundle build tĩnh (Vite xuất vào /assets) là public;
                        // dữ liệu chỉ nằm sau /api/** — vẫn fail closed.
                        .requestMatchers("/", "/index.html", "/assets/**", "/favicon.ico", "/error").permitAll()
                        .anyRequest().authenticated())
                .oauth2Login(withDefaults())
                .oauth2ResourceServer(o -> o.jwt(withDefaults()))
                .logout(l -> l.logoutSuccessHandler(oidcLogout))
                .csrf(c -> c.ignoringRequestMatchers("/api/**", "/logout"));
        return http.build();
    }
}
