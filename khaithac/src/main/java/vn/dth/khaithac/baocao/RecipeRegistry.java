package vn.dth.khaithac.baocao;

import tools.jackson.databind.ObjectMapper;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Nạp toàn bộ recipe từ classpath:recipes/*.json lúc khởi động. */
@Component
public class RecipeRegistry {

    private final Map<String, Recipe> recipes = new LinkedHashMap<>();

    public RecipeRegistry() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Resource[] files = new PathMatchingResourcePatternResolver()
                .getResources("classpath:recipes/*.json");
        for (Resource f : files) {
            try (var in = f.getInputStream()) {
                Recipe r = mapper.readValue(in, Recipe.class);
                recipes.put(r.maBieu(), r);
            }
        }
    }

    public Recipe get(String maBieu) {
        return recipes.get(maBieu);
    }

    public List<Recipe> all() {
        return List.copyOf(recipes.values());
    }
}
