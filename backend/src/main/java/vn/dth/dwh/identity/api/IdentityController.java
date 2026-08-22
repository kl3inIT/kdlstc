package vn.dth.dwh.identity.api;

import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
public class IdentityController {

    @GetMapping("/me")
    CurrentUserResponse currentUser(Authentication authentication) {
        String displayName = authentication.getPrincipal() instanceof OidcUser oidcUser
                ? oidcUser.getFullName()
                : authentication.getName();
        List<String> roles = authentication.getAuthorities().stream()
                .map(authority -> authority.getAuthority())
                .filter(authority -> authority.startsWith("ROLE_"))
                .map(authority -> authority.substring("ROLE_".length()))
                .sorted()
                .toList();
        return new CurrentUserResponse(authentication.getName(), displayName, roles);
    }

    @GetMapping("/csrf")
    CsrfTokenResponse csrf(CsrfToken token) {
        return new CsrfTokenResponse(token.getHeaderName(), token.getParameterName(), token.getToken());
    }
}
