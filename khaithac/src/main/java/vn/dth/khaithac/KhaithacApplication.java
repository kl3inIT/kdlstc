package vn.dth.khaithac;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * App khai thác — phase 1: dựng biểu từ API viên gạch của các app Jmix,
 * ẩn cột theo quyền (lan truyền qua công thức), BFF đăng nhập Keycloak.
 * Kiến trúc học theo D:\OrgMemory (BFF session, fail-closed, token relay).
 */
@SpringBootApplication
public class KhaithacApplication {
    public static void main(String[] args) {
        SpringApplication.run(KhaithacApplication.class, args);
    }
}
