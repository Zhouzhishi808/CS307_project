package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.service.RecipeService;
import io.sustc.service.ReviewService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class ReviewServiceImpl implements ReviewService {


    @Override
    @Transactional
    public long addReview(AuthInfo auth, long recipeId, int rating, String review) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Transactional
    public void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Transactional
    public void deleteReview(AuthInfo auth, long recipeId, long reviewId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Transactional
    public long likeReview(AuthInfo auth, long reviewId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Transactional
    public long unlikeReview(AuthInfo auth, long reviewId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Transactional
    public RecipeRecord refreshRecipeAggregatedRating(long recipeId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

}