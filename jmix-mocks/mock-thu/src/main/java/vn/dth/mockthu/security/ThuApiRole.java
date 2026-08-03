package vn.dth.mockthu.security;

import io.jmix.rest.security.role.RestMinimalRole;
import io.jmix.security.model.EntityAttributePolicyAction;
import io.jmix.security.model.EntityPolicyAction;
import io.jmix.security.role.annotation.EntityAttributePolicy;
import io.jmix.security.role.annotation.EntityPolicy;
import io.jmix.security.role.annotation.ResourceRole;
import io.jmix.security.role.annotation.SpecificPolicy;
import vn.dth.mockthu.entity.DanhMuc;
import vn.dth.mockthu.entity.ThuDuToan;
import vn.dth.mockthu.entity.ThuGiaoDich;

/**
 * Quyền truy cập API tổng hợp Thu. CODE phải trùng tên role trong Keycloak:
 * user có realm role `thu-api` trong token thì ClaimsRolesMapper gán role này;
 * thiếu role → controller trả 403 → tầng khai thác ẩn nhóm cột nguồn THU.
 */
@ResourceRole(name = "Thu API", code = ThuApiRole.CODE, scope = "API")
public interface ThuApiRole extends RestMinimalRole {
    String CODE = "thu-api";

    @SpecificPolicy(resources = "thu.api")
    @EntityPolicy(entityClass = ThuGiaoDich.class, actions = EntityPolicyAction.READ)
    @EntityAttributePolicy(entityClass = ThuGiaoDich.class, attributes = "*", action = EntityAttributePolicyAction.VIEW)
    @EntityPolicy(entityClass = ThuDuToan.class, actions = EntityPolicyAction.READ)
    @EntityAttributePolicy(entityClass = ThuDuToan.class, attributes = "*", action = EntityAttributePolicyAction.VIEW)
    @EntityPolicy(entityClass = DanhMuc.class, actions = EntityPolicyAction.READ)
    @EntityAttributePolicy(entityClass = DanhMuc.class, attributes = "*", action = EntityAttributePolicyAction.VIEW)
    void thuApi();
}
