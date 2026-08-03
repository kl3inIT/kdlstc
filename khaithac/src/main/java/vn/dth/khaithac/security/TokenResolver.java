package vn.dth.khaithac.security;

import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.client.OAuth2AuthorizeRequest;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;

/**
 * Lấy access token của user hiện tại để relay xuống API Jmix:
 *  - Bearer vào (agent) → dùng lại chính token đó.
 *  - Session BFF (browser) → lấy qua OAuth2AuthorizedClientManager:
 *    token hết hạn (Keycloak mặc định 5 phút) sẽ được TỰ REFRESH bằng
 *    refresh token trong session — không được đọc token tĩnh từ store,
 *    sẽ dính 401 giữa phiên. Refresh hết cứu nổi (SSO idle quá hạn) → null
 *    → controller trả 401 → frontend đưa user đăng nhập lại.
 */
@Component
public class TokenResolver {

    private final OAuth2AuthorizedClientManager authorizedClientManager;

    public TokenResolver(OAuth2AuthorizedClientManager authorizedClientManager) {
        this.authorizedClientManager = authorizedClientManager;
    }

    public String resolve(Authentication auth) {
        if (auth instanceof JwtAuthenticationToken jwt) {
            return jwt.getToken().getTokenValue();
        }
        if (auth instanceof OAuth2AuthenticationToken oauth) {
            try {
                OAuth2AuthorizedClient client = authorizedClientManager.authorize(
                        OAuth2AuthorizeRequest
                                .withClientRegistrationId(oauth.getAuthorizedClientRegistrationId())
                                .principal(oauth)
                                .build());
                return client != null ? client.getAccessToken().getTokenValue() : null;
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }
}
