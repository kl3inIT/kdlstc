package vn.dth.mockchi.security;

import io.jmix.rest.security.role.RestMinimalRole;
import io.jmix.security.model.EntityAttributePolicyAction;
import io.jmix.security.model.EntityPolicyAction;
import io.jmix.security.role.annotation.EntityAttributePolicy;
import io.jmix.security.role.annotation.EntityPolicy;
import io.jmix.security.role.annotation.ResourceRole;
import io.jmix.security.role.annotation.SpecificPolicy;
import vn.dth.mockchi.entity.ChiDuToan;
import vn.dth.mockchi.entity.ChiGiaoDich;
import vn.dth.mockchi.entity.DanhMuc;

/**
 * Quyền truy cập API tổng hợp Chi. CODE trùng tên realm role trong Keycloak
 * (`chi-api`); thiếu role → 403 → tầng khai thác ẩn nhóm cột nguồn CHI.
 */
@ResourceRole(name = "Chi API", code = ChiApiRole.CODE, scope = "API")
public interface ChiApiRole extends RestMinimalRole {
    String CODE = "chi-api";

    @SpecificPolicy(resources = "chi.api")
    @EntityPolicy(entityClass = ChiGiaoDich.class, actions = EntityPolicyAction.READ)
    @EntityAttributePolicy(entityClass = ChiGiaoDich.class, attributes = "*", action = EntityAttributePolicyAction.VIEW)
    @EntityPolicy(entityClass = ChiDuToan.class, actions = EntityPolicyAction.READ)
    @EntityAttributePolicy(entityClass = ChiDuToan.class, attributes = "*", action = EntityAttributePolicyAction.VIEW)
    @EntityPolicy(entityClass = DanhMuc.class, actions = EntityPolicyAction.READ)
    @EntityAttributePolicy(entityClass = DanhMuc.class, attributes = "*", action = EntityAttributePolicyAction.VIEW)
    void chiApi();
}
