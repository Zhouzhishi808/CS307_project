package io.sustc.controller;

import io.sustc.dto.*;
import io.sustc.service.ReviewService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/reviews")
public class ReviewController {

    @Autowired
    private ReviewService reviewService;

    @PostMapping
    public Object addReview(@RequestBody Map<String, Object> request) {
        AuthInfo auth = new AuthInfo();
        auth.setAuthorId(((Number) request.get("authorId")).longValue());
        auth.setPassword((String) request.get("password"));
        
        long recipeId = ((Number) request.get("recipeId")).longValue();
        int rating = (Integer) request.get("rating");
        String review = (String) request.get("review");
        
        long reviewId = reviewService.addReview(auth, recipeId, rating, review);
        return Map.of("reviewId", reviewId);
    }

    @PutMapping("/{reviewId}")
    public String editReview(@PathVariable long reviewId, @RequestBody Map<String, Object> request) {
        AuthInfo auth = new AuthInfo();
        auth.setAuthorId(((Number) request.get("authorId")).longValue());
        auth.setPassword((String) request.get("password"));
        
        long recipeId = ((Number) request.get("recipeId")).longValue();
        int rating = (Integer) request.get("rating");
        String review = (String) request.get("review");
        
        reviewService.editReview(auth, recipeId, reviewId, rating, review);
        return "修改成功";
    }

    @DeleteMapping("/{reviewId}")
    public String deleteReview(@PathVariable long reviewId, @RequestBody Map<String, Object> request) {
        AuthInfo auth = new AuthInfo();
        auth.setAuthorId(((Number) request.get("authorId")).longValue());
        auth.setPassword((String) request.get("password"));
        
        long recipeId = ((Number) request.get("recipeId")).longValue();
        
        reviewService.deleteReview(auth, recipeId, reviewId);
        return "删除成功";
    }

    @PostMapping("/{reviewId}/like")
    public Object likeReview(@PathVariable long reviewId, @RequestBody AuthInfo auth) {
        long likeCount = reviewService.likeReview(auth, reviewId);
        return Map.of("likeCount", likeCount);
    }

    @DeleteMapping("/{reviewId}/like")
    public Object unlikeReview(@PathVariable long reviewId, @RequestBody AuthInfo auth) {
        long likeCount = reviewService.unlikeReview(auth, reviewId);
        return Map.of("likeCount", likeCount);
    }

    @GetMapping("/recipe/{recipeId}")
    public PageResult<ReviewRecord> listByRecipe(@PathVariable long recipeId,
                                               @RequestParam(defaultValue = "1") int page,
                                               @RequestParam(defaultValue = "10") int size,
                                               @RequestParam(defaultValue = "date_desc") String sort) {
        return reviewService.listByRecipe(recipeId, page, size, sort);
    }
}