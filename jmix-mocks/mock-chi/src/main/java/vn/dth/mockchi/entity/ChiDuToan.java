package vn.dth.mockchi.entity;

import io.jmix.core.metamodel.annotation.JmixEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * Dự toán chi theo chiều. Dự toán hiện hành = dauNam + dieuChinh — consumer tính.
 */
@JmixEntity
@Table(name = "CHI_DU_TOAN")
@Entity
public class ChiDuToan {
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

    @Column(name = "DU_TOAN_DAU_NAM", nullable = false)
    private Long duToanDauNam;

    @Column(name = "DIEU_CHINH_TRONG_NAM", nullable = false)
    private Long dieuChinhTrongNam = 0L;

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
    public Long getDuToanDauNam() { return duToanDauNam; }
    public void setDuToanDauNam(Long duToanDauNam) { this.duToanDauNam = duToanDauNam; }
    public Long getDieuChinhTrongNam() { return dieuChinhTrongNam; }
    public void setDieuChinhTrongNam(Long dieuChinhTrongNam) { this.dieuChinhTrongNam = dieuChinhTrongNam; }
}
