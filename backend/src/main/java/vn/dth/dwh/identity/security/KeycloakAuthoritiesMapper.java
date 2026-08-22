package vn.dth.dwh.identity.security;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.authority.mapping.GrantedAuthoritiesMapper;
import org.springframework.security.oauth2.core.oidc.user.OidcUserAuthority;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static vn.dth.dwh.identity.security.OperationsPermissions.OVERVIEW_READ;
import static vn.dth.dwh.identity.security.OperationsPermissions.PIPELINE_READ;
import static vn.dth.dwh.identity.security.OperationsPermissions.PIPELINE_TRIGGER;

@Component
public class KeycloakAuthoritiesMapper implements GrantedAuthoritiesMapper {

    private static final Map<String, Set<String>> PERMISSIONS_BY_ROLE = Map.of(
            "quan_tri", Set.of(OVERVIEW_READ, PIPELINE_READ, PIPELINE_TRIGGER),
            "chuyen_vien", Set.of(OVERVIEW_READ, PIPELINE_READ, PIPELINE_TRIGGER),
            "lanh_dao", Set.of(OVERVIEW_READ, PIPELINE_READ),
            "ke_toan_dv", Set.of(OVERVIEW_READ, PIPELINE_READ)
    );

    @Override
    public Collection<? extends GrantedAuthority> mapAuthorities(
            Collection<? extends GrantedAuthority> authorities
    ) {
        Set<GrantedAuthority> mapped = new HashSet<>(authorities);
        authorities.stream()
                .filter(OidcUserAuthority.class::isInstance)
                .map(OidcUserAuthority.class::cast)
                .flatMap(authority -> roles(authority).stream())
                .forEach(role -> {
                    mapped.add(new SimpleGrantedAuthority("ROLE_" + role));
                    PERMISSIONS_BY_ROLE.getOrDefault(role, Set.of()).stream()
                            .map(SimpleGrantedAuthority::new)
                            .forEach(mapped::add);
                });
        return Set.copyOf(mapped);
    }

    private Set<String> roles(OidcUserAuthority authority) {
        Set<String> roles = new HashSet<>(roles(authority.getIdToken().getClaims()));
        if (authority.getUserInfo() != null) {
            roles.addAll(roles(authority.getUserInfo().getClaims()));
        }
        return Set.copyOf(roles);
    }

    private Set<String> roles(Map<String, Object> claims) {
        Set<String> roles = new HashSet<>();
        addRoles(roles, claims.get("roles"));
        if (claims.get("realm_access") instanceof Map<?, ?> realmAccess) {
            addRoles(roles, realmAccess.get("roles"));
        }
        return Set.copyOf(roles);
    }

    private void addRoles(Set<String> target, Object rawRoles) {
        if (rawRoles instanceof Collection<?> values) {
            values.stream().map(Object::toString).filter(value -> !value.isBlank()).forEach(target::add);
        }
    }
}
