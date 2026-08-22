package vn.dth.dwh.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Transactional
class OperationsApiIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void protectsAndServesTheReactBffContracts() throws Exception {
        mockMvc.perform(get("/api/platform/overview"))
                .andExpect(status().isUnauthorized());

        mockMvc.perform(get("/api/platform/overview")
                        .with(user("operator").authorities(() -> "platform.overview.read")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value("province-warehouse"))
                .andExpect(jsonPath("$.steps.length()").value(7));

        mockMvc.perform(get("/api/csrf").with(user("operator")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.headerName").isNotEmpty())
                .andExpect(jsonPath("$.token").isNotEmpty());

        mockMvc.perform(post("/api/pipelines/imate/runs")
                        .with(user("operator").authorities(() -> "pipeline.trigger"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"scope\":\"iMate · toàn bộ chuỗi\"}"))
                .andExpect(status().isForbidden());

        mockMvc.perform(post("/api/pipelines/imate/runs")
                        .with(user("operator").authorities(() -> "pipeline.trigger"))
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"scope\":\"iMate · toàn bộ chuỗi\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.data.status").value("running"))
                .andExpect(jsonPath("$.data.correlationId").isNotEmpty())
                .andExpect(jsonPath("$.data.steps.length()").value(8));

        mockMvc.perform(get("/api/pipelines/imate/runs")
                        .with(user("operator").authorities(() -> "pipeline.read")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].steps.length()").value(8));

        mockMvc.perform(post("/api/pipelines/imate/runs")
                        .with(user("viewer").authorities(() -> "pipeline.read"))
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"scope\":\"iMate · toàn bộ chuỗi\"}"))
                .andExpect(status().isForbidden());
    }

    @Test
    void returnsIdentityFromTheAuthenticatedSession() throws Exception {
        mockMvc.perform(get("/api/me").with(user("operator").roles("chuyen_vien")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.username").value("operator"))
                .andExpect(jsonPath("$.roles[0]").value("chuyen_vien"));
    }
}
