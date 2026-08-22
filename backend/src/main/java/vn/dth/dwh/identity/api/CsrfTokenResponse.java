package vn.dth.dwh.identity.api;

public record CsrfTokenResponse(
        String headerName,
        String parameterName,
        String token
) {
}
