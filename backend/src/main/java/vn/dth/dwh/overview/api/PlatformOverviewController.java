package vn.dth.dwh.overview.api;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import vn.dth.dwh.overview.application.PlatformOverviewService;

import static vn.dth.dwh.identity.security.OperationsPermissions.OVERVIEW_READ;

@RestController
@RequestMapping("/api/platform/overview")
public class PlatformOverviewController {

    private final PlatformOverviewService overviewService;

    public PlatformOverviewController(PlatformOverviewService overviewService) {
        this.overviewService = overviewService;
    }

    @GetMapping
    @PreAuthorize("hasAuthority('" + OVERVIEW_READ + "')")
    PlatformOverviewResponse overview() {
        return overviewService.overview();
    }
}
