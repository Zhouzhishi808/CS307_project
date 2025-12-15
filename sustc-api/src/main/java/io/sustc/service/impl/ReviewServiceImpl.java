package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.service.ReviewService;

public class ReviewServiceImpl implements ReviewService {
    @Override
    public long addReview(AuthInfo auth, long recipeId, int rating, String review) {

    }

    @Override
    public void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review) {

    }

    @Override
    public void deleteReview(AuthInfo auth, long recipeId, long reviewId) {

    }

    @Override
    public long likeReview(AuthInfo auth, long reviewId) {
        return 0;
    }

    @Override
    public long unlikeReview(AuthInfo auth, long reviewId) {
        return 0;
    }

    @Override
    public PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort) {
        return null;
    }

    @Override
    public RecipeRecord refreshRecipeAggregatedRating(long recipeId) {
        return null;
    }
}
