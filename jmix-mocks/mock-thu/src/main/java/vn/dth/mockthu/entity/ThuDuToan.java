package vn.dth.mockthu.entity;

import io.jmix.core.metamodel.annotation.JmixEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * Dự toán thu theo từng chiều: LOAI_CHIEU (DIA_BAN / CO_QUAN_THU / NGUON_THU)
 * + KHOA (mã của chiều đó). API trả số thô, tỷ lệ hoàn thành do consumer tính.
 */
@JmixEntity
@Table(name = "THU_DU_TOAN")
@Entity
public class ThuDuToan {
    @Id
    @Column(name = "ID", nullable = false)
    private Long id;

    @Column(name = "VERSION", nullable = false)
    @Version
    private Integer version;

    @Column(name = "NAM", nullable = false)
    private Integer nam;

    @Column(name = "LOAI_CHIEU", length = 20, nullable = false)
    private String loaiChieu;

    @Column(name = "KHOA", length = 20, nullable = false)
    private String khoa;

    @Column(name = "DU_TOAN", nullable = false)
    private Long duToan;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Integer getVersion() { return version; }
    public void setVersion(Integer version) { this.version = version; }
    public Integer getNam() { return nam; }
    public void setNam(Integer nam) { this.nam = nam; }
    public String getLoaiChieu() { return loaiChieu; }
    public void setLoaiChieu(String loaiChieu) { this.loaiChieu = loaiChieu; }
    public String getKhoa() { return khoa; }
    public void setKhoa(String khoa) { this.khoa = khoa; }
    public Long getDuToan() { return duToan; }
    public void setDuToan(Long duToan) { this.duToan = duToan; }
}
