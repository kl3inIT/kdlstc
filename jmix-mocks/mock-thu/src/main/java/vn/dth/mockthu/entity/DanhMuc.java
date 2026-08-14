package vn.dth.mockthu.entity;

import io.jmix.core.metamodel.annotation.InstanceName;
import io.jmix.core.metamodel.annotation.JmixEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * Danh mục dùng chung (mã ↔ tên, cây cha-con): DIA_BAN, CO_QUAN_THU,
 * NGUON_THU, TIEU_MUC...
 */
@JmixEntity
@Table(name = "DANH_MUC")
@Entity
public class DanhMuc {
    @Id
    @Column(name = "ID", nullable = false)
    private Long id;

    @Column(name = "VERSION", nullable = false)
    @Version
    private Integer version;

    @Column(name = "LOAI", length = 20, nullable = false)
    private String loai;

    @Column(name = "MA", length = 20, nullable = false)
    private String ma;

    @InstanceName
    @Column(name = "TEN", length = 255, nullable = false)
    private String ten;

    @Column(name = "MA_CHA", length = 20)
    private String maCha;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Integer getVersion() { return version; }
    public void setVersion(Integer version) { this.version = version; }
    public String getLoai() { return loai; }
    public void setLoai(String loai) { this.loai = loai; }
    public String getMa() { return ma; }
    public void setMa(String ma) { this.ma = ma; }
    public String getTen() { return ten; }
    public void setTen(String ten) { this.ten = ten; }
    public String getMaCha() { return maCha; }
    public void setMaCha(String maCha) { this.maCha = maCha; }
}
