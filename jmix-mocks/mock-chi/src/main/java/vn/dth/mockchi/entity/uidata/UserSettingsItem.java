package vn.dth.mockchi.entity.uidata;

import io.jmix.core.entity.annotation.JmixGeneratedValue;
import io.jmix.core.entity.annotation.SystemLevel;
import io.jmix.core.metamodel.annotation.JmixEntity;
import jakarta.persistence.*;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

// tag::entity[]
@Entity(name = "flowui_UserSettingsItem")
@JmixEntity
@Table(name = "FLOWUI_USER_SETTINGS")
@SystemLevel
public class UserSettingsItem implements Serializable {
    private static final long serialVersionUID = -5262191502816639662L;

    @Id
    @Column(name = "ID")
    @JmixGeneratedValue
    protected UUID id;

    @CreatedDate
    @Column(name = "CREATE_TS")
    private Date createTs;

    @CreatedBy
    @Column(name = "CREATED_BY")
    private String createdBy;

    @Column(name = "USERNAME")
    private String username;

    @Column(name = "KEY_")
    private String key;

    @Lob
    @Column(name = "VALUE_")
    private String value;

    // getters and setters
    // end::entity[]

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public Date getCreateTs() {
        return createTs;
    }

    public void setCreateTs(Date createTs) {
        this.createTs = createTs;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}