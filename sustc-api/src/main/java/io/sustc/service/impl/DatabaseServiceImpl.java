package io.sustc.service.impl;

import io.sustc.dto.ReviewRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.RecipeRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;



/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-depen
     * dency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        //replace this with your own student IDs in your group
        return Arrays.asList(12413009,12410708);
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    @Transactional
    public void importData(
            List<ReviewRecord> reviewRecords,
            List<UserRecord> userRecords,
            List<RecipeRecord> recipeRecords) {

        // ddl to create tables.
        createTables();

        // 优化：先导入数据，后创建索引和触发器以提升性能
        importUsersOptimized(userRecords);
        importUserFollowsOptimized(userRecords);
        importRecipesOptimized(recipeRecords);
        importRecipeIngredientsOptimized(recipeRecords);
        importReviewsOptimized(reviewRecords);
        importReviewLikesOptimized(reviewRecords);

        createTriggers();
        createViews();
        createIndexes();
    }
    
    private void createPerformanceProcedures() {
        // 高性能用户查询存储过程
        String userProc = "CREATE OR REPLACE FUNCTION get_user_with_follows(user_id BIGINT)\n" +
                "RETURNS TABLE(\n" +
                "  AuthorId BIGINT,\n" +
                "  AuthorName VARCHAR,\n" +
                "  Gender VARCHAR,\n" +
                "  Age INTEGER,\n" +
                "  Followers INTEGER,\n" +
                "  Following INTEGER,\n" +
                "  Password VARCHAR,\n" +
                "  IsDeleted BOOLEAN,\n" +
                "  FollowerUsers BIGINT[],\n" +
                "  FollowingUsers BIGINT[]\n" +
                ")\n" +
                "LANGUAGE plpgsql\n" +
                "AS $$\n" +
                "BEGIN\n" +
                "  RETURN QUERY\n" +
                "  SELECT u.AuthorId, u.AuthorName, u.Gender, u.Age, u.Followers, u.Following, u.Password, u.IsDeleted,\n" +
                "         ARRAY(SELECT uf1.FollowerId FROM user_follows uf1 WHERE uf1.FollowingId = user_id ORDER BY uf1.FollowerId),\n" +
                "         ARRAY(SELECT uf2.FollowingId FROM user_follows uf2 WHERE uf2.FollowerId = user_id ORDER BY uf2.FollowingId)\n" +
                "  FROM users u\n" +
                "  WHERE u.AuthorId = user_id;\n" +
                "END;\n" +
                "$$;";
        
        // 高性能菜谱查询存储过程
        String recipeProc = "CREATE OR REPLACE FUNCTION get_recipe_with_ingredients(recipe_id BIGINT)\n" +
                "RETURNS TABLE(\n" +
                "  RecipeId BIGINT,\n" +
                "  Name VARCHAR,\n" +
                "  AuthorId BIGINT,\n" +
                "  AuthorName VARCHAR,\n" +
                "  CookTime VARCHAR,\n" +
                "  PrepTime VARCHAR,\n" +
                "  TotalTime VARCHAR,\n" +
                "  DatePublished TIMESTAMP,\n" +
                "  Description TEXT,\n" +
                "  RecipeCategory VARCHAR,\n" +
                "  AggregatedRating DECIMAL,\n" +
                "  ReviewCount INTEGER,\n" +
                "  Calories DECIMAL,\n" +
                "  FatContent DECIMAL,\n" +
                "  SaturatedFatContent DECIMAL,\n" +
                "  CholesterolContent DECIMAL,\n" +
                "  SodiumContent DECIMAL,\n" +
                "  CarbohydrateContent DECIMAL,\n" +
                "  FiberContent DECIMAL,\n" +
                "  SugarContent DECIMAL,\n" +
                "  ProteinContent DECIMAL,\n" +
                "  RecipeServings VARCHAR,\n" +
                "  RecipeYield VARCHAR,\n" +
                "  RecipeIngredientParts VARCHAR[]\n" +
                ")\n" +
                "LANGUAGE plpgsql\n" +
                "AS $$\n" +
                "BEGIN\n" +
                "  RETURN QUERY\n" +
                "  SELECT r.*, u.AuthorName,\n" +
                "         ARRAY(SELECT ri.IngredientPart FROM recipe_ingredients ri WHERE ri.RecipeId = recipe_id ORDER BY LOWER(ri.IngredientPart) COLLATE \"C\")\n" +
                "  FROM recipes r\n" +
                "  LEFT JOIN users u ON r.AuthorId = u.AuthorId\n" +
                "  WHERE r.RecipeId = recipe_id;\n" +
                "END;\n" +
                "$$;";
        
        try {
            jdbcTemplate.execute(userProc);
            jdbcTemplate.execute(recipeProc);
        } catch (Exception e) {
            log.warn("Failed to create performance procedures: {}", e.getMessage());
        }
    }

    public void createTriggers() {
    String t1= "CREATE OR REPLACE FUNCTION update_follow_counts()\n" +
                "RETURNS TRIGGER AS $$\n" +
                "BEGIN\n" +
                "    IF TG_OP = 'INSERT' THEN\n" +
                "        UPDATE users SET Followers = Followers + 1 WHERE AuthorId = NEW.FollowingId;\n" +
                "        UPDATE users SET Following = Following + 1 WHERE AuthorId = NEW.FollowerId;\n" +
                "    ELSIF TG_OP = 'DELETE' THEN\n" +
                "        UPDATE users SET Followers = Followers - 1 WHERE AuthorId = OLD.FollowingId;\n" +
                "        UPDATE users SET Following = Following - 1 WHERE AuthorId = OLD.FollowerId;\n" +
                "    END IF;\n" +
                "    RETURN NULL;\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql;\n" +
                "\n" +
                "CREATE TRIGGER trg_update_follow_counts\n" +
                "AFTER INSERT OR DELETE ON user_follows\n" +
                "FOR EACH ROW EXECUTE FUNCTION update_follow_counts();";
        //aggregateRating更新
        String t2= "CREATE OR REPLACE FUNCTION refresh_recipe_rating()\n" +
                "RETURNS TRIGGER AS $$\n" +
                "DECLARE\n" +
                "    target_recipe_id BIGINT;\n" +
                "BEGIN\n" +
                "    IF TG_OP = 'DELETE' THEN\n" +
                "        target_recipe_id := OLD.RecipeId;\n" +
                "    ELSE\n" +
                "        target_recipe_id := NEW.RecipeId;\n" +
                "    END IF;\n" +
                "    \n" +
                "    UPDATE recipes SET\n" +
                "        AggregatedRating = (SELECT ROUND(AVG(Rating)::numeric, 2) FROM reviews WHERE RecipeId = target_recipe_id),\n" +
                "        ReviewCount = (SELECT COUNT(*) FROM reviews WHERE RecipeId = target_recipe_id)\n" +
                "    WHERE RecipeId = target_recipe_id;\n" +
                "    \n" +
                "    RETURN NULL;\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql;\n" +
                "\n" +
                "CREATE TRIGGER trg_refresh_recipe_rating\n" +
                "AFTER INSERT OR UPDATE OR DELETE ON reviews\n" +
                "FOR EACH ROW EXECUTE FUNCTION refresh_recipe_rating();";
                jdbcTemplate.execute(t1);
                jdbcTemplate.execute(t2);
    }

    private void createViews(){
        String V1="CREATE OR REPLACE VIEW v_user_full_info AS\n" +
                "SELECT \n" +
                "    u.AuthorId,\n" +
                "    u.AuthorName,\n" +
                "    u.Gender,\n" +
                "    u.Age,\n" +
                "    u.Followers,\n" +
                "    u.Following,\n" +
                "    u.Password,\n" +
                "    u.IsDeleted,\n" +
                "    ARRAY_AGG(DISTINCT uf_followers.FollowerId ORDER BY uf_followers.FollowerId) FILTER (WHERE uf_followers.FollowerId IS NOT NULL) AS FollowerUsers,\n" +
                "    ARRAY_AGG(DISTINCT uf_following.FollowingId ORDER BY uf_following.FollowingId) FILTER (WHERE uf_following.FollowingId IS NOT NULL) AS FollowingUsers\n" +
                "FROM users u\n" +
                "LEFT JOIN user_follows uf_followers ON u.AuthorId = uf_followers.FollowingId\n" +
                "LEFT JOIN user_follows uf_following ON u.AuthorId = uf_following.FollowerId\n" +
                "GROUP BY u.AuthorId, u.AuthorName, u.Gender, u.Age, u.Followers, u.Following, u.Password, u.IsDeleted;";//用户完整信息视图，用于简化 UserService.getById() 的实现

        String V2="CREATE OR REPLACE VIEW v_recipe_full_info AS\n" +
                "SELECT \n" +
                "    r.*,\n" +
                "    ARRAY_AGG(ri.IngredientPart ORDER BY LOWER(ri.IngredientPart) COLLATE \"C\") FILTER (WHERE ri.IngredientPart IS NOT NULL) AS RecipeIngredientParts\n" +
                "FROM recipes r\n" +
                "LEFT JOIN recipe_ingredients ri ON r.RecipeId = ri.RecipeId\n" +
                "GROUP BY r.RecipeId;";//菜谱完整信息视图，用于简化RecipeService.getRecipeById() 和 searchRecipes() 的实现。
        
        // 性能优化视图：预计算评论点赞数
        String V3="CREATE OR REPLACE VIEW v_review_with_likes AS\n" +
                "SELECT \n" +
                "    r.*,\n" +
                "    u.AuthorName,\n" +
                "    COALESCE(COUNT(rl.ReviewId), 0) as like_count,\n" +
                "    ARRAY_AGG(rl.AuthorId ORDER BY rl.AuthorId) FILTER (WHERE rl.AuthorId IS NOT NULL) AS likes_array\n" +
                "FROM reviews r\n" +
                "LEFT JOIN users u ON r.AuthorId = u.AuthorId\n" +
                "LEFT JOIN review_likes rl ON r.ReviewId = rl.ReviewId\n" +
                "GROUP BY r.ReviewId, r.RecipeId, r.AuthorId, r.Rating, r.Review, r.DateSubmitted, r.DateModified, u.AuthorName;";
        
        jdbcTemplate.execute(V1);
        jdbcTemplate.execute(V2);
        jdbcTemplate.execute(V3);
        
        createPerformanceProcedures();
    }
    public void createIndexes(){
        // 索引创建语句数组
        String[] indexSQLs = {
                //user 表索引
                "CREATE INDEX IF NOT EXISTS USER_PWD ON USERS(AUTHORID,PASSWORD)" ,
                "CREATE INDEX IF NOT EXISTS idx_users_active ON users(AuthorId) WHERE IsDeleted = false",
                
                //user_follows表索引
                "CREATE INDEX IF NOT EXISTS idx_user_follows_follower ON user_follows(FollowerId)",
                "CREATE INDEX IF NOT EXISTS idx_user_follows_following ON user_follows(FollowingId)",
                "CREATE INDEX IF NOT EXISTS idx_user_follows_composite ON user_follows(FollowerId, FollowingId)",

                //recipes表索引
                "CREATE INDEX IF NOT EXISTS idx_name ON recipes(recipeid,name)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_category ON recipes(RecipeCategory)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_rating ON recipes(AggregatedRating DESC NULLS LAST)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_date ON recipes(DatePublished DESC)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_calories ON recipes(Calories)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_author ON recipes(AuthorId)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_category_rating ON recipes(RecipeCategory, AggregatedRating DESC NULLS LAST)",

                //reviews表索引
                "CREATE INDEX IF NOT EXISTS idx_reviews_recipe ON reviews(RecipeId)",
                "CREATE INDEX IF NOT EXISTS idx_reviews_author ON reviews(AuthorId)",
                "CREATE INDEX IF NOT EXISTS idx_reviews_date ON reviews(DateModified DESC)",
                "CREATE INDEX IF NOT EXISTS idx_reviews_recipe_date ON reviews(RecipeId, DateModified DESC)",

                //review_likes表索引
                "CREATE INDEX IF NOT EXISTS idx_review_likes_review ON review_likes(ReviewId)",
                "CREATE INDEX IF NOT EXISTS idx_review_likes_author ON review_likes(AuthorId)",
                "CREATE INDEX IF NOT EXISTS idx_review_likes_composite ON review_likes(AuthorId, ReviewId)",

                //recipe_ingredients表索引
                "CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_recipe ON recipe_ingredients(RecipeId)",

                //复合索引
                "CREATE INDEX IF NOT EXISTS idx_recipes_author_date ON recipes(AuthorId, DatePublished DESC)",
                "CREATE INDEX IF NOT EXISTS idx_feed_optimization ON user_follows(FollowerId) INCLUDE (FollowingId)",

                //启用pg_trgm扩展
                "CREATE EXTENSION IF NOT EXISTS pg_trgm",

                //全文搜索索引
                "CREATE INDEX IF NOT EXISTS idx_recipes_name_trgm ON recipes USING gin(Name gin_trgm_ops)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_desc_trgm ON recipes USING gin(Description gin_trgm_ops)"
        };

        // 逐条执行索引创建语句
        for (String sql : indexSQLs) {
            try {
                jdbcTemplate.execute(sql);
            } catch (Exception e) {
                log.warn("Failed to create index: {} - {}", sql, e.getMessage());
            }
        }
    }

    private static final int BATCH_SIZE = 20000; // 优化的批处理大小

    private void importUsersOptimized(List<UserRecord> userRecords) {
        String sql = "INSERT INTO users (AuthorId, AuthorName, Gender, Age, " +
                "Followers, Following, Password, IsDeleted) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        processBatches(userRecords, BATCH_SIZE, (batch) -> {
            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    UserRecord user = batch.get(i);
                    ps.setLong(1, user.getAuthorId());
                    ps.setString(2, user.getAuthorName());
                    ps.setString(3, user.getGender());
                    ps.setInt(4, user.getAge());
                    ps.setInt(5, user.getFollowers());
                    ps.setInt(6, user.getFollowing());
                    ps.setString(7, user.getPassword());
                    ps.setBoolean(8, user.isDeleted());
                }

                @Override
                public int getBatchSize() {
                    return batch.size();
                }
            });
        });
    }

    private void importUserFollowsOptimized(List<UserRecord> userRecords) {
        List<Object[]> allFollows = new ArrayList<>();
        
        for (UserRecord user : userRecords) {
            if (user.getFollowingUsers() != null) {
                for (long followingId : user.getFollowingUsers()) {
                    allFollows.add(new Object[]{user.getAuthorId(), followingId});
                }
            }
        }

        if (!allFollows.isEmpty()) {
            String sql = "INSERT INTO user_follows (FollowerId, FollowingId) " +
                    "VALUES (?, ?) ON CONFLICT DO NOTHING";
            
            processBatches(allFollows, BATCH_SIZE, (batch) -> {
                jdbcTemplate.batchUpdate(sql, batch);
            });
        }
    }

    private void importRecipesOptimized(List<RecipeRecord> recipeRecords) {
        String sql = "INSERT INTO recipes " +
                "(RecipeId, Name, AuthorId, CookTime, PrepTime, TotalTime, " +
                "DatePublished, Description, RecipeCategory, AggregatedRating, ReviewCount, " +
                "Calories, FatContent, SaturatedFatContent, CholesterolContent, " +
                "SodiumContent, CarbohydrateContent, FiberContent, SugarContent, " +
                "ProteinContent, RecipeServings, RecipeYield) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        processBatches(recipeRecords, BATCH_SIZE, (batch) -> {
            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    RecipeRecord recipe = batch.get(i);
                    ps.setLong(1, recipe.getRecipeId());
                    ps.setString(2, recipe.getName());
                    ps.setLong(3, recipe.getAuthorId());
                    ps.setString(4, recipe.getCookTime());
                    ps.setString(5, recipe.getPrepTime());
                    ps.setString(6, recipe.getTotalTime());
                    ps.setTimestamp(7, recipe.getDatePublished());
                    ps.setString(8, recipe.getDescription());
                    ps.setString(9, recipe.getRecipeCategory());

                    if (recipe.getAggregatedRating() > 0) {
                        ps.setFloat(10, recipe.getAggregatedRating());
                    } else {
                        ps.setNull(10, java.sql.Types.DECIMAL);
                    }
                    ps.setInt(11, recipe.getReviewCount());

                    setFloatOrNull(ps, 12, recipe.getCalories());
                    setFloatOrNull(ps, 13, recipe.getFatContent());
                    setFloatOrNull(ps, 14, recipe.getSaturatedFatContent());
                    setFloatOrNull(ps, 15, recipe.getCholesterolContent());
                    setFloatOrNull(ps, 16, recipe.getSodiumContent());
                    setFloatOrNull(ps, 17, recipe.getCarbohydrateContent());
                    setFloatOrNull(ps, 18, recipe.getFiberContent());
                    setFloatOrNull(ps, 19, recipe.getSugarContent());
                    setFloatOrNull(ps, 20, recipe.getProteinContent());

                    ps.setString(21, String.valueOf(recipe.getRecipeServings()));
                    ps.setString(22, recipe.getRecipeYield());
                }

                @Override
                public int getBatchSize() {
                    return batch.size();
                }
            });
        });
    }

    private void importRecipeIngredientsOptimized(List<RecipeRecord> recipeRecords) {
        List<Object[]> allIngredients = new ArrayList<>();

        for (RecipeRecord recipe : recipeRecords) {
            if (recipe.getRecipeIngredientParts() != null) {
                for (String ingredient : recipe.getRecipeIngredientParts()) {
                    allIngredients.add(new Object[]{
                            recipe.getRecipeId(),
                            ingredient
                    });
                }
            }
        }
        
        if (!allIngredients.isEmpty()) {
            String sql = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) " +
                    "VALUES (?, ?) ON CONFLICT DO NOTHING";
            
            processBatches(allIngredients, BATCH_SIZE, (batch) -> {
                jdbcTemplate.batchUpdate(sql, batch);
            });
        }
    }

    private void importReviewsOptimized(List<ReviewRecord> reviewRecords) {
        String sql = "INSERT INTO reviews " +
                "(ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        processBatches(reviewRecords, BATCH_SIZE, (batch) -> {
            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    ReviewRecord review = batch.get(i);
                    ps.setLong(1, review.getReviewId());
                    ps.setLong(2, review.getRecipeId());
                    ps.setLong(3, review.getAuthorId());
                    ps.setFloat(4, review.getRating());
                    ps.setString(5, review.getReview());
                    ps.setTimestamp(6, review.getDateSubmitted());
                    ps.setTimestamp(7, review.getDateModified());
                }
                
                @Override
                public int getBatchSize() {
                    return batch.size();
                }
            });
        });
    }

    private void importReviewLikesOptimized(List<ReviewRecord> reviewRecords) {
        List<Object[]> allLikes = new ArrayList<>();
        
        for (ReviewRecord review : reviewRecords) {
            if (review.getLikes() != null) {
                for (long userId : review.getLikes()) {
                    allLikes.add(new Object[]{
                            review.getReviewId(),
                            userId
                    });
                }
            }
        }

        if (!allLikes.isEmpty()) {
            String sql = "INSERT INTO review_likes (ReviewId, AuthorId) " +
                    "VALUES (?, ?) ON CONFLICT DO NOTHING";
            
            processBatches(allLikes, BATCH_SIZE, (batch) -> {
                jdbcTemplate.batchUpdate(sql, batch);
            });
        }
    }

    private <T> void processBatches(List<T> items, int batchSize, java.util.function.Consumer<List<T>> processor) {
        for (int i = 0; i < items.size(); i += batchSize) {
            int end = Math.min(i + batchSize, items.size());
            List<T> batch = items.subList(i, end);
            processor.accept(batch);
        }
    }

    private void importUsers(List<UserRecord> userRecords) {
        String sql = "INSERT INTO users (AuthorId, AuthorName, Gender, Age, " +
                "Followers, Following, Password, IsDeleted) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                UserRecord user = userRecords.get(i);
                ps.setLong(1, user.getAuthorId());
                ps.setString(2, user.getAuthorName());
                ps.setString(3, user.getGender());
                ps.setInt(4, user.getAge());
                ps.setInt(5, user.getFollowers());
                ps.setInt(6, user.getFollowing());
                ps.setString(7, user.getPassword());  // 可能为null
                ps.setBoolean(8, user.isDeleted());
            }

            @Override
            public int getBatchSize() {
                return userRecords.size();
            }
        });
    }

    private void importUserFollows(List<UserRecord> userRecords) {
        List<Object[]> followBatch = new ArrayList<>();

        for (UserRecord user : userRecords) {
            if (user.getFollowingUsers() != null) {
                for (long followingId : user.getFollowingUsers()) {
                    followBatch.add(new Object[]{user.getAuthorId(), followingId});
                }
            }
        }

        if (!followBatch.isEmpty()) {
            String sql = "INSERT INTO user_follows (FollowerId, FollowingId) " +
                    "VALUES (?, ?) ON CONFLICT DO NOTHING";
            jdbcTemplate.batchUpdate(sql, followBatch);
        }
    }


    private void importRecipes(List<RecipeRecord> recipeRecords) {
        String sql = "INSERT INTO recipes " +
                "(RecipeId, Name, AuthorId, CookTime, PrepTime, TotalTime, " +
                "DatePublished, Description, RecipeCategory, AggregatedRating, ReviewCount, " +
                "Calories, FatContent, SaturatedFatContent, CholesterolContent, " +
                "SodiumContent, CarbohydrateContent, FiberContent, SugarContent, " +
                "ProteinContent, RecipeServings, RecipeYield) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                RecipeRecord recipe = recipeRecords.get(i);
                ps.setLong(1, recipe.getRecipeId());
                ps.setString(2, recipe.getName());
                ps.setLong(3, recipe.getAuthorId());
                ps.setString(4, recipe.getCookTime());
                ps.setString(5, recipe.getPrepTime());
                ps.setString(6, recipe.getTotalTime());
                ps.setTimestamp(7, recipe.getDatePublished());
                ps.setString(8, recipe.getDescription());
                ps.setString(9, recipe.getRecipeCategory());

                if (recipe.getAggregatedRating() > 0) {
                    ps.setFloat(10, recipe.getAggregatedRating());
                } else {
                    ps.setNull(10, java.sql.Types.DECIMAL);
                }
                ps.setInt(11, recipe.getReviewCount());

                setFloatOrNull(ps, 12, recipe.getCalories());
                setFloatOrNull(ps, 13, recipe.getFatContent());
                setFloatOrNull(ps, 14, recipe.getSaturatedFatContent());
                setFloatOrNull(ps, 15, recipe.getCholesterolContent());
                setFloatOrNull(ps, 16, recipe.getSodiumContent());
                setFloatOrNull(ps, 17, recipe.getCarbohydrateContent());
                setFloatOrNull(ps, 18, recipe.getFiberContent());
                setFloatOrNull(ps, 19, recipe.getSugarContent());
                setFloatOrNull(ps, 20, recipe.getProteinContent());

                ps.setString(21, String.valueOf(recipe.getRecipeServings()));
                ps.setString(22, recipe.getRecipeYield());
            }

            @Override
            public int getBatchSize() {
                return recipeRecords.size();
            }
        });
    }

    private void setFloatOrNull(PreparedStatement ps, int index, float value) throws SQLException {
        if (value > 0) {
            ps.setFloat(index, value);
        } else {
            ps.setNull(index, java.sql.Types.DECIMAL);
        }
    }
    private void importRecipeIngredients(List<RecipeRecord> recipeRecords) {
        List<Object[]> ingredientBatch = new ArrayList<>();

        for (RecipeRecord recipe : recipeRecords) {
            if (recipe.getRecipeIngredientParts() != null) {
                for (String ingredient : recipe.getRecipeIngredientParts()) {
                    ingredientBatch.add(new Object[]{
                            recipe.getRecipeId(),
                            ingredient
                    });
                }
            }
        }
        if (!ingredientBatch.isEmpty()) {
            String sql = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) " +
                    "VALUES (?, ?) ON CONFLICT DO NOTHING";
            jdbcTemplate.batchUpdate(sql, ingredientBatch);
        }
    }

    private void importReviews(List<ReviewRecord> reviewRecords) {
        String sql = "INSERT INTO reviews " +
                "(ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ReviewRecord review = reviewRecords.get(i);
                ps.setLong(1, review.getReviewId());
                ps.setLong(2, review.getRecipeId());
                ps.setLong(3, review.getAuthorId());
                ps.setFloat(4, review.getRating());
                ps.setString(5, review.getReview());
                ps.setTimestamp(6, review.getDateSubmitted());
                ps.setTimestamp(7, review.getDateModified());
            }
            @Override
            public int getBatchSize() {
                return reviewRecords.size();
            }
        });
    }

    private void importReviewLikes(List<ReviewRecord> reviewRecords) {
        List<Object[]> likesBatch = new ArrayList<>();
        for (ReviewRecord review : reviewRecords) {
            if (review.getLikes() != null) {
                for (long userId : review.getLikes()) {
                    likesBatch.add(new Object[]{
                            review.getReviewId(),
                            userId
                    });
                }
            }
        }

        if (!likesBatch.isEmpty()) {
            String sql = "INSERT INTO review_likes (ReviewId, AuthorId) " +
                    "VALUES (?, ?) ON CONFLICT DO NOTHING";
            jdbcTemplate.batchUpdate(sql, likesBatch);
        }
    }

    private void createTables() {
        String[] createTableSQLs = {
                // 创建users表
                "CREATE TABLE IF NOT EXISTS public.users (" +
                        "    AuthorId BIGINT PRIMARY KEY, " +
                        "    AuthorName VARCHAR(255) NOT NULL, " +
                        "    Gender VARCHAR(10) CHECK (Gender IN ('Male', 'Female')), " +
                        "    Age INTEGER CHECK (Age > 0), " +
                        "    Followers INTEGER DEFAULT 0 CHECK (Followers >= 0), " +
                        "    Following INTEGER DEFAULT 0 CHECK (Following >= 0), " +
                        "    Password VARCHAR(255), " +
                        "    IsDeleted BOOLEAN DEFAULT FALSE" +
                        ")",

                // 创建recipes表
                "CREATE TABLE IF NOT EXISTS public.recipes (" +
                        "    RecipeId BIGINT PRIMARY KEY, " +
                        "    Name VARCHAR(500) NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    CookTime VARCHAR(50), " +
                        "    PrepTime VARCHAR(50), " +
                        "    TotalTime VARCHAR(50), " +
                        "    DatePublished TIMESTAMP, " +
                        "    Description TEXT, " +
                        "    RecipeCategory VARCHAR(255), " +
                        "    AggregatedRating DECIMAL(3,2) CHECK (AggregatedRating >= 0 AND AggregatedRating <= 5), " +
                        "    ReviewCount INTEGER DEFAULT 0 CHECK (ReviewCount >= 0), " +
                        "    Calories DECIMAL(10,2), " +
                        "    FatContent DECIMAL(10,2), " +
                        "    SaturatedFatContent DECIMAL(10,2), " +
                        "    CholesterolContent DECIMAL(10,2), " +
                        "    SodiumContent DECIMAL(10,2), " +
                        "    CarbohydrateContent DECIMAL(10,2), " +
                        "    FiberContent DECIMAL(10,2), " +
                        "    SugarContent DECIMAL(10,2), " +
                        "    ProteinContent DECIMAL(10,2), " +
                        "    RecipeServings VARCHAR(100), " +
                        "    RecipeYield VARCHAR(100), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES public.users(AuthorId)" +
                        ")",

                // 创建reviews表
                "CREATE TABLE IF NOT EXISTS public.reviews (" +
                        "    ReviewId BIGINT PRIMARY KEY, " +
                        "    RecipeId BIGINT NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    Rating INTEGER, " +
                        "    Review TEXT, " +
                        "    DateSubmitted TIMESTAMP, " +
                        "    DateModified TIMESTAMP, " +
                        "    FOREIGN KEY (RecipeId) REFERENCES public.recipes(RecipeId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES public.users(AuthorId)" +
                        ")",

                // 创建recipe_ingredients表
                "CREATE TABLE IF NOT EXISTS public.recipe_ingredients (" +
                        "    RecipeId BIGINT, " +
                        "    IngredientPart VARCHAR(500), " +
                        "    PRIMARY KEY (RecipeId, IngredientPart), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES public.recipes(RecipeId)" +
                        ")",

                // 创建review_likes表
                "CREATE TABLE IF NOT EXISTS public.review_likes (" +
                        "    ReviewId BIGINT, " +
                        "    AuthorId BIGINT, " +
                        "    PRIMARY KEY (ReviewId, AuthorId), " +
                        "    FOREIGN KEY (ReviewId) REFERENCES public.reviews(ReviewId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES public.users(AuthorId)" +
                        ")",

                // 创建user_follows表
                "CREATE TABLE IF NOT EXISTS public.user_follows (" +
                        "    FollowerId BIGINT, " +
                        "    FollowingId BIGINT, " +
                        "    PRIMARY KEY (FollowerId, FollowingId), " +
                        "    FOREIGN KEY (FollowerId) REFERENCES public.users(AuthorId), " +
                        "    FOREIGN KEY (FollowingId) REFERENCES public.users(AuthorId), " +
                        "    CHECK (FollowerId != FollowingId)" +
                        ")"
        };

        for (String sql : createTableSQLs) {
            jdbcTemplate.execute(sql);
        }
    }



    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void drop() {
//        jdbcTemplate.execute("DROP TRIGGER IF EXISTS trg_update_follow_counts ON user_follows");
//        jdbcTemplate.execute("DROP TRIGGER IF EXISTS trg_refresh_recipe_rating ON reviews");
//        jdbcTemplate.execute("DROP FUNCTION IF EXISTS update_follow_counts() CASCADE");
//        jdbcTemplate.execute("DROP FUNCTION IF EXISTS refresh_recipe_rating() CASCADE");
//        jdbcTemplate.execute("DROP VIEW IF EXISTS v_user_full_info CASCADE");
//        jdbcTemplate.execute("DROP VIEW IF EXISTS v_recipe_full_info CASCADE");
        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'DROP TABLE IF EXISTS ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // jdbcTemplate.execute("DROP EXTENSION IF EXISTS pg_trgm CASCADE");
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
