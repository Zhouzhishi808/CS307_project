package io.sustc.controller;

import io.sustc.dto.*;
import io.sustc.service.RecipeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/recipes")
public class RecipeController {

    @Autowired
    private RecipeService recipeService;

    @GetMapping("/{recipeId}")
    public RecipeRecord getRecipeById(@PathVariable long recipeId) {
        return recipeService.getRecipeById(recipeId);
    }

    @GetMapping("/search")
    public PageResult<RecipeRecord> searchRecipes(@RequestParam(required = false) String keyword,
                                                 @RequestParam(required = false) String category,
                                                 @RequestParam(required = false) Double minRating,
                                                 @RequestParam(defaultValue = "1") Integer page,
                                                 @RequestParam(defaultValue = "10") Integer size,
                                                 @RequestParam(required = false) String sort) {
        return recipeService.searchRecipes(keyword, category, minRating, page, size, sort);
    }

    @PostMapping
    public Object createRecipe(@RequestBody Map<String, Object> request) {
        AuthInfo auth = new AuthInfo();
        auth.setAuthorId(((Number) request.get("authorId")).longValue());
        auth.setPassword((String) request.get("password"));
        
        RecipeRecord recipe = new RecipeRecord();
        recipe.setName((String) request.get("name"));
        recipe.setDescription((String) request.get("description"));
        recipe.setRecipeCategory((String) request.get("recipeCategory"));
        
        long recipeId = recipeService.createRecipe(recipe, auth);
        return Map.of("recipeId", recipeId);
    }

    @DeleteMapping("/{recipeId}")
    public String deleteRecipe(@PathVariable long recipeId, @RequestBody AuthInfo auth) {
        recipeService.deleteRecipe(recipeId, auth);
        return "删除成功";
    }

    @PutMapping("/{recipeId}/times")
    public String updateTimes(@PathVariable long recipeId, @RequestBody Map<String, Object> request) {
        AuthInfo auth = new AuthInfo();
        auth.setAuthorId(((Number) request.get("authorId")).longValue());
        auth.setPassword((String) request.get("password"));
        
        String cookTimeIso = (String) request.get("cookTimeIso");
        String prepTimeIso = (String) request.get("prepTimeIso");
        
        recipeService.updateTimes(auth, recipeId, cookTimeIso, prepTimeIso);
        return "更新成功";
    }

    @GetMapping("/closest-calorie-pair")
    public Map<String, Object> getClosestCaloriePair() {
        return recipeService.getClosestCaloriePair();
    }

    @GetMapping("/top-complex")
    public List<Map<String, Object>> getTopComplexRecipes() {
        return recipeService.getTop3MostComplexRecipesByIngredients();
    }
}