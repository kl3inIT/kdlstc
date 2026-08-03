package vn.dth.khaithac.baocao;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import vn.dth.khaithac.nguon.NguonLoiException;
import vn.dth.khaithac.security.TokenResolver;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class BaoCaoController {

    private final RecipeRegistry registry;
    private final RecipeEngine engine;
    private final TokenResolver tokenResolver;

    public BaoCaoController(RecipeRegistry registry, RecipeEngine engine, TokenResolver tokenResolver) {
        this.registry = registry;
        this.engine = engine;
        this.tokenResolver = tokenResolver;
    }

    @GetMapping("/me")
    public Map<String, Object> me(Authentication auth) {
        return Map.of("username", auth.getName());
    }

    @GetMapping("/bao-cao")
    public List<Map<String, String>> danhSach() {
        return registry.all().stream()
                .map(r -> Map.of("ma_bieu", r.maBieu(), "ten_bieu", r.tenBieu()))
                .toList();
    }

    @GetMapping("/bao-cao/{maBieu}")
    public ResponseEntity<?> dungBieu(@PathVariable String maBieu,
                                      @RequestParam("tu_ngay") String tuNgay,
                                      @RequestParam("den_ngay") String denNgay,
                                      Authentication auth) {
        Recipe recipe = registry.get(maBieu);
        if (recipe == null) {
            return ResponseEntity.notFound().build();
        }
        String token = tokenResolver.resolve(auth);
        if (token == null) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(Map.of("error", "khong_lay_duoc_token"));
        }
        return ResponseEntity.ok(engine.dungBieu(
                recipe, LocalDate.parse(tuNgay), LocalDate.parse(denNgay), token));
    }

    /** Nguồn lỗi thật → 502, nói rõ nguồn nào — KHÔNG ẩn cột im lặng. */
    @ExceptionHandler(NguonLoiException.class)
    public ResponseEntity<?> nguonLoi(NguonLoiException e) {
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(Map.of(
                "error", "nguon_loi",
                "nguon", e.getNguon(),
                "message", e.getMessage()));
    }
}
