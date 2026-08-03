package vn.dth.mockthu.entity;

import io.jmix.core.metamodel.annotation.JmixEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.LocalDate;

/**
 * Giao dịch thu NSNN — bảng nguồn cho API tổng hợp.
 * Mock v1 giữ MLNS rút gọn (chương + tiểu mục) — đủ để validate group_by,
 * không nhằm đủ 5 cấp như bản thật.
 */
@JmixEntity
@Table(name = "THU_GIAO_DICH")
@Entity
public class ThuGiaoDich {
    @Id
    @Column(name = "ID", nullable = false)
    private Long id;

    @Column(name = "VERSION", nullable = false)
    @Version
    private Integer version;

    @Column(name = "NGAY", nullable = false)
    private LocalDate ngay;

    @Column(name = "MA_CO_QUAN_THU", length = 10, nullable = false)
    private String maCoQuanThu;

    @Column(name = "MA_DIA_BAN", length = 10, nullable = false)
    private String maDiaBan;

    @Column(name = "MA_CHUONG", length = 3)
    private String maChuong;

    @Column(name = "MA_TIEU_MUC", length = 4)
    private String maTieuMuc;

    @Column(name = "MA_NGUON_THU", length = 10)
    private String maNguonThu;

    @Column(name = "MA_SO_THUE", length = 14)
    private String maSoThue;

    @Column(name = "SO_TIEN", nullable = false)
    private Long soTien;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Integer getVersion() { return version; }
    public void setVersion(Integer version) { this.version = version; }
    public LocalDate getNgay() { return ngay; }
    public void setNgay(LocalDate ngay) { this.ngay = ngay; }
    public String getMaCoQuanThu() { return maCoQuanThu; }
    public void setMaCoQuanThu(String maCoQuanThu) { this.maCoQuanThu = maCoQuanThu; }
    public String getMaDiaBan() { return maDiaBan; }
    public void setMaDiaBan(String maDiaBan) { this.maDiaBan = maDiaBan; }
    public String getMaChuong() { return maChuong; }
    public void setMaChuong(String maChuong) { this.maChuong = maChuong; }
    public String getMaTieuMuc() { return maTieuMuc; }
    public void setMaTieuMuc(String maTieuMuc) { this.maTieuMuc = maTieuMuc; }
    public String getMaNguonThu() { return maNguonThu; }
    public void setMaNguonThu(String maNguonThu) { this.maNguonThu = maNguonThu; }
    public String getMaSoThue() { return maSoThue; }
    public void setMaSoThue(String maSoThue) { this.maSoThue = maSoThue; }
    public Long getSoTien() { return soTien; }
    public void setSoTien(Long soTien) { this.soTien = soTien; }
}
