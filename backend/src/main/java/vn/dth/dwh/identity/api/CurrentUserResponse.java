package vn.dth.dwh.identity.api;

import java.util.List;

public record CurrentUserResponse(
        String username,
        String displayName,
        List<String> roles
) {
}
