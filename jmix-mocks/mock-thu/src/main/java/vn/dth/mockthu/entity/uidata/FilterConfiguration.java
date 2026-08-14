package vn.dth.mockthu.entity.uidata;

import io.jmix.core.annotation.TenantId;
import io.jmix.core.entity.annotation.JmixGeneratedValue;
import io.jmix.core.entity.annotation.SystemLevel;
import io.jmix.core.metamodel.annotation.InstanceName;
import io.jmix.core.metamodel.annotation.JmixEntity;
import io.jmix.core.metamodel.annotation.JmixProperty;
import jakarta.persistence.*;

import java.io.Serializable;
import java.util.UUID;

// tag::entity[]
@JmixEntity
@Entity(name = "flowui_FilterConfiguration")
@Table(name = "FLOWUI_FILTER_CONFIGURATION")
@SystemLevel
public class FilterConfiguration implements Serializable {

    @Id
    @Column(name = "ID", nullable = false)
    @JmixGeneratedValue
    protected UUID id;

    @Column(name = "COMPONENT_ID", nullable = false)
    protected String componentId;

    @Column(name = "CONFIGURATION_ID", nullable = false)
    protected String configurationId;

    @InstanceName
    @Column(name = "NAME")
    protected String name;

    @Column(name = "USERNAME")
    protected String username;

    @Column(name = "DEFAULT_FOR_ALL")
    protected Boolean defaultForAll = false;

    @Column(name = "ROOT_CONDITION")
    @Lob
    protected String rootCondition;

    @TenantId
    @Column(name = "SYS_TENANT_ID")
    protected String sysTenantId;

    @JmixProperty
    @Transient
    protected Boolean defaultForMe = false;

    // getters and setters
    // end::entity[]
    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    public String getConfigurationId() {
        return configurationId;
    }

    public void setConfigurationId(String configurationId) {
        this.configurationId = configurationId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Boolean getDefaultForAll() {
        return defaultForAll;
    }

    public void setDefaultForAll(Boolean defaultForAll) {
        this.defaultForAll = defaultForAll;
    }

    public String getRootCondition() {
        return rootCondition;
    }

    public void setRootCondition(String rootCondition) {
        this.rootCondition = rootCondition;
    }

    public String getSysTenantId() {
        return sysTenantId;
    }

    public void setSysTenantId(String sysTenantId) {
        this.sysTenantId = sysTenantId;
    }

    public Boolean getDefaultForMe() {
        return defaultForMe;
    }

    public void setDefaultForMe(Boolean defaultForMe) {
        this.defaultForMe = defaultForMe;
    }
}
