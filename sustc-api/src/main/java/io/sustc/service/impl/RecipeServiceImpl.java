package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.RecipeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import java.util.*;
@Service
@Slf4j
public class RecipeServiceImpl implements RecipeService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public String getNameFromID(long id) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public RecipeRecord getRecipeById(long recipeId) {
        //占位方法，用于过测试
        try {
            String sql = "SELECT * FROM public.recipes WHERE RecipeId = ?";
            RecipeRecord recipe = jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                RecipeRecord r = new RecipeRecord();
                r.setRecipeId(rs.getLong("RecipeId"));
                r.setName(rs.getString("Name"));
                r.setAuthorId(rs.getLong("AuthorId"));
                r.setCookTime(rs.getString("CookTime"));
                r.setPrepTime(rs.getString("PrepTime"));
                r.setTotalTime(rs.getString("TotalTime"));
                r.setDatePublished(rs.getTimestamp("DatePublished"));
                r.setDescription(rs.getString("Description"));
                r.setRecipeCategory(rs.getString("RecipeCategory"));
                r.setAggregatedRating((float) rs.getDouble("AggregatedRating"));
                r.setReviewCount(rs.getInt("ReviewCount"));
                r.setCalories((float) rs.getDouble("Calories"));
                r.setFatContent((float) rs.getDouble("FatContent"));
                r.setSaturatedFatContent((float) rs.getDouble("SaturatedFatContent"));
                r.setCholesterolContent((float) rs.getDouble("CholesterolContent"));
                r.setSodiumContent((float) rs.getDouble("SodiumContent"));
                r.setCarbohydrateContent((float) rs.getDouble("CarbohydrateContent"));
                r.setFiberContent((float) rs.getDouble("FiberContent"));
                r.setSugarContent((float) rs.getDouble("SugarContent"));
                r.setProteinContent((float) rs.getDouble("ProteinContent"));
                r.setRecipeServings(Integer.parseInt(rs.getString("RecipeServings")));
                r.setRecipeYield(rs.getString("RecipeYield"));
                return r;
            }, recipeId);


            String ingredientSql = "SELECT IngredientPart FROM public.recipe_ingredients WHERE RecipeId = ?";
            List<String> ingredients = jdbcTemplate.queryForList(ingredientSql, String.class, recipeId);
//            recipe.setRecipeIngredientParts(ingredients);

            return recipe;
        } catch (Exception e) {
            return null;
        }
    }


    @Override
    public PageResult<RecipeRecord> searchRecipes(String keyword, String category, Double minRating,
                                                  Integer page, Integer size, String sort) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public long createRecipe(RecipeRecord dto, AuthInfo auth) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void deleteRecipe(long recipeId, AuthInfo auth) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void updateTimes(AuthInfo auth, long recipeId, String cookTimeIso, String prepTimeIso) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Map<String, Object> getClosestCaloriePair() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public List<Map<String, Object>> getTop3MostComplexRecipesByIngredients() {
        throw new UnsupportedOperationException("Not implemented yet");
    }


}