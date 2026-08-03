package vn.dth.khaithac.ai;

import org.springframework.stereotype.Component;
import vn.dth.khaithac.baocao.RecipeEngine;
import vn.dth.khaithac.baocao.RecipeRegistry;
import vn.dth.khaithac.nguon.NguonClient;

/** Tạo bộ tool cho từng request, token user bake sẵn — an toàn với streaming. */
@Component
public class AiToolsFactory {

    private final NguonClient nguonClient;
    private final RecipeRegistry recipeRegistry;
    private final RecipeEngine recipeEngine;

    public AiToolsFactory(NguonClient nguonClient, RecipeRegistry recipeRegistry, RecipeEngine recipeEngine) {
        this.nguonClient = nguonClient;
        this.recipeRegistry = recipeRegistry;
        this.recipeEngine = recipeEngine;
    }

    public AiTools create(String token) {
        return new AiTools(nguonClient, recipeRegistry, recipeEngine, token);
    }
}
