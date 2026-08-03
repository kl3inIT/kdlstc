package vn.dth.mockchi.entity;

import io.jmix.core.metamodel.annotation.JmixEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.LocalDate;

/**
 * Giao dịch chi NSNN — bảng nguồn cho API tổng hợp CHI.
 * laTamUng = true: khoản tạm ứng (đo lường du_tam_ung); false: thực chi.
 */
@JmixEntity
@Table(name = "CHI_GIAO_DICH")
@Entity
public class ChiGiaoDich {
    @Id
    @Column(name = "ID", nullable = false)
    private Long id;

    @Column(name = "VERSION", nullable = false)
    @Version
    private Integer version;

    @Column(name = "NGAY", nullable = false)
    private LocalDate ngay;

    @Column(name = "MA_DVQHNS", length = 10, nullable = false)
    private String maDvqhns;

    @Column(name = "MA_DIA_BAN", length = 10, nullable = false)
    private String maDiaBan;

    @Column(name = "MA_LINH_VUC_CHI", length = 10)
    private String maLinhVucChi;

    @Column(name = "MA_NGUON_KP", length = 10)
    private String maNguonKp;

    @Column(name = "MA_CHUONG", length = 3)
    private String maChuong;

    @Column(name = "MA_TIEU_MUC", length = 4)
    private String maTieuMuc;

    @Column(name = "LA_TAM_UNG", nullable = false)
    private Boolean laTamUng = false;

    @Column(name = "SO_TIEN", nullable = false)
    private Long soTien;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Integer getVersion() { return version; }
    public void setVersion(Integer version) { this.version = version; }
    public LocalDate getNgay() { return ngay; }
    public void setNgay(LocalDate ngay) { this.ngay = ngay; }
    public String getMaDvqhns() { return maDvqhns; }
    public void setMaDvqhns(String maDvqhns) { this.maDvqhns = maDvqhns; }
    public String getMaDiaBan() { return maDiaBan; }
    public void setMaDiaBan(String maDiaBan) { this.maDiaBan = maDiaBan; }
    public String getMaLinhVucChi() { return maLinhVucChi; }
    public void setMaLinhVucChi(String maLinhVucChi) { this.maLinhVucChi = maLinhVucChi; }
    public String getMaNguonKp() { return maNguonKp; }
    public void setMaNguonKp(String maNguonKp) { this.maNguonKp = maNguonKp; }
    public String getMaChuong() { return maChuong; }
    public void setMaChuong(String maChuong) { this.maChuong = maChuong; }
    public String getMaTieuMuc() { return maTieuMuc; }
    public void setMaTieuMuc(String maTieuMuc) { this.maTieuMuc = maTieuMuc; }
    public Boolean getLaTamUng() { return laTamUng; }
    public void setLaTamUng(Boolean laTamUng) { this.laTamUng = laTamUng; }
    public Long getSoTien() { return soTien; }
    public void setSoTien(Long soTien) { this.soTien = soTien; }
}
