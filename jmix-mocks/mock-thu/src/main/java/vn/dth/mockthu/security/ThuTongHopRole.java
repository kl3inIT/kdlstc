package vn.dth.mockthu.security;

import io.jmix.rest.security.role.RestMinimalRole;
import io.jmix.security.role.annotation.ResourceRole;
import io.jmix.security.role.annotation.SpecificPolicy;

/**
 * Quyền HẸP: chỉ xem số TỔNG HỢP thu (endpoint /rest/thu/tong-hop) + danh mục.
 * KHÔNG bao gồm dự toán. Dành cho user ngoài ngành thu nhưng cần vài trường
 * tổng (vd cân đối thu-chi địa bàn). Gán role Keycloak cùng mã: `thu-tong-hop`.
 */
@ResourceRole(name = "Thu tong hop (hẹp)", code = ThuTongHopRole.CODE, scope = "API")
public interface ThuTongHopRole extends RestMinimalRole {
    String CODE = "thu-tong-hop";

    @SpecificPolicy(resources = "thu.tonghop")
    void thuTongHop();
}
