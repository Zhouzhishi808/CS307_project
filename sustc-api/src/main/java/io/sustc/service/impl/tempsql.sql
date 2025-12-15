
DROP TABLE IF EXISTS public.review_likes CASCADE;
DROP TABLE IF EXISTS public.recipe_ingredients CASCADE;
DROP TABLE IF EXISTS public.reviews CASCADE;
DROP TABLE IF EXISTS public.user_follows CASCADE;
DROP TABLE IF EXISTS public.recipes CASCADE;
DROP TABLE IF EXISTS public.users CASCADE;

-- ========================================
-- 1. CREATE TABLES
-- ========================================

-- Users table: stores user information
CREATE TABLE public.users (
    AuthorId BIGINT PRIMARY KEY,
    AuthorName VARCHAR(255) NOT NULL,
    Gender VARCHAR(10) CHECK (Gender IN ('Male', 'Female')),
    Age INTEGER CHECK (Age > 0),
    Followers INTEGER DEFAULT 0 CHECK (Followers >= 0),
    Following INTEGER DEFAULT 0 CHECK (Following >= 0),
    Password VARCHAR(255),
    IsDeleted BOOLEAN DEFAULT FALSE
);

-- Recipes table: stores recipe information
CREATE TABLE public.recipes (
    RecipeId BIGINT PRIMARY KEY,
    Name VARCHAR(500) NOT NULL,
    AuthorId BIGINT NOT NULL,
    CookTime VARCHAR(50),
    PrepTime VARCHAR(50),
    TotalTime VARCHAR(50),
    DatePublished TIMESTAMP,
    Description TEXT,
    RecipeCategory VARCHAR(255),
    AggregatedRating DECIMAL(3,2) CHECK (AggregatedRating >= 0 AND AggregatedRating <= 5),
    ReviewCount INTEGER DEFAULT 0 CHECK (ReviewCount >= 0),
    Calories DECIMAL(10,2),
    FatContent DECIMAL(10,2),
    SaturatedFatContent DECIMAL(10,2),
    CholesterolContent DECIMAL(10,2),
    SodiumContent DECIMAL(10,2),
    CarbohydrateContent DECIMAL(10,2),
    FiberContent DECIMAL(10,2),
    SugarContent DECIMAL(10,2),
    ProteinContent DECIMAL(10,2),
    RecipeServings VARCHAR(100),
    RecipeYield VARCHAR(100),
    FOREIGN KEY (AuthorId) REFERENCES public.users(AuthorId)
);

-- Reviews table: stores recipe reviews
CREATE TABLE public.reviews (
    ReviewId BIGINT PRIMARY KEY,
    RecipeId BIGINT NOT NULL,
    AuthorId BIGINT NOT NULL,
    Rating INTEGER,
    Review TEXT,
    DateSubmitted TIMESTAMP,
    DateModified TIMESTAMP,
    FOREIGN KEY (RecipeId) REFERENCES public.recipes(RecipeId),
    FOREIGN KEY (AuthorId) REFERENCES public.users(AuthorId)
);

-- Recipe ingredients table: stores ingredients for each recipe
CREATE TABLE public.recipe_ingredients (
    RecipeId BIGINT,
    IngredientPart VARCHAR(500),
    PRIMARY KEY (RecipeId, IngredientPart),
    FOREIGN KEY (RecipeId) REFERENCES public.recipes(RecipeId)
);

-- Review likes table: stores which users liked which reviews
CREATE TABLE public.review_likes (
    ReviewId BIGINT,
    AuthorId BIGINT,
    PRIMARY KEY (ReviewId, AuthorId),
    FOREIGN KEY (ReviewId) REFERENCES public.reviews(ReviewId),
    FOREIGN KEY (AuthorId) REFERENCES public.users(AuthorId)
);

-- User follows table: stores follower/following relationships
CREATE TABLE public.user_follows (
    FollowerId BIGINT,
    FollowingId BIGINT,
    PRIMARY KEY (FollowerId, FollowingId),
    FOREIGN KEY (FollowerId) REFERENCES public.users(AuthorId),
    FOREIGN KEY (FollowingId) REFERENCES public.users(AuthorId),
    CHECK (FollowerId != FollowingId)
);

-- ========================================
-- 2. CREATE TRIGGERS
-- ========================================

-- Trigger to automatically update follower/following counts
CREATE OR REPLACE FUNCTION update_follow_counts()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE users SET Followers = Followers + 1 WHERE AuthorId = NEW.FollowingId;
        UPDATE users SET Following = Following + 1 WHERE AuthorId = NEW.FollowerId;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE users SET Followers = Followers - 1 WHERE AuthorId = OLD.FollowingId;
        UPDATE users SET Following = Following - 1 WHERE AuthorId = OLD.FollowerId;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_follow_counts
AFTER INSERT OR DELETE ON user_follows
FOR EACH ROW EXECUTE FUNCTION update_follow_counts();

-- Trigger to automatically update recipe ratings
CREATE OR REPLACE FUNCTION refresh_recipe_rating()
RETURNS TRIGGER AS $$
DECLARE
    target_recipe_id BIGINT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        target_recipe_id := OLD.RecipeId;
    ELSE
        target_recipe_id := NEW.RecipeId;
    END IF;

    UPDATE recipes SET
        AggregatedRating = (SELECT ROUND(AVG(Rating)::numeric, 2) FROM reviews WHERE RecipeId = target_recipe_id),
        ReviewCount = (SELECT COUNT(*) FROM reviews WHERE RecipeId = target_recipe_id)
    WHERE RecipeId = target_recipe_id;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_refresh_recipe_rating
AFTER INSERT OR UPDATE OR DELETE ON reviews
FOR EACH ROW EXECUTE FUNCTION refresh_recipe_rating();

-- ========================================
-- 3. CREATE VIEWS
-- ========================================

-- View: User full information with follower/following arrays
CREATE OR REPLACE VIEW v_user_full_info AS
SELECT
    u.AuthorId,
    u.AuthorName,
    u.Gender,
    u.Age,
    u.Followers,
    u.Following,
    u.IsDeleted,
    ARRAY_AGG(DISTINCT uf_followers.FollowerId) FILTER (WHERE uf_followers.FollowerId IS NOT NULL) AS FollowerUsers,
    ARRAY_AGG(DISTINCT uf_following.FollowingId) FILTER (WHERE uf_following.FollowingId IS NOT NULL) AS FollowingUsers
FROM users u
LEFT JOIN user_follows uf_followers ON u.AuthorId = uf_followers.FollowingId
LEFT JOIN user_follows uf_following ON u.AuthorId = uf_following.FollowerId
GROUP BY u.AuthorId;

-- View: Recipe full information with ingredients array
CREATE OR REPLACE VIEW v_recipe_full_info AS
SELECT
    r.*,
    ARRAY_AGG(ri.IngredientPart ORDER BY ri.IngredientPart) FILTER (WHERE ri.IngredientPart IS NOT NULL) AS RecipeIngredientParts
FROM recipes r
LEFT JOIN recipe_ingredients ri ON r.RecipeId = ri.RecipeId
GROUP BY r.RecipeId;

-- ========================================
-- 4. CREATE INDEXES
-- ========================================

-- Enable pg_trgm extension for fuzzy search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- User follows table indexes
CREATE INDEX idx_user_follows_follower ON user_follows(FollowerId);
CREATE INDEX idx_user_follows_following ON user_follows(FollowingId);

-- Recipes table indexes
CREATE INDEX idx_recipes_category ON recipes(RecipeCategory);
CREATE INDEX idx_recipes_rating ON recipes(AggregatedRating DESC);
CREATE INDEX idx_recipes_date ON recipes(DatePublished DESC);
CREATE INDEX idx_recipes_calories ON recipes(Calories);
CREATE INDEX idx_recipes_author ON recipes(AuthorId);
CREATE INDEX idx_recipes_author_date ON recipes(AuthorId, DatePublished DESC);

-- Full-text search indexes for recipes
CREATE INDEX idx_recipes_name_trgm ON recipes USING gin(Name gin_trgm_ops);
CREATE INDEX idx_recipes_desc_trgm ON recipes USING gin(Description gin_trgm_ops);

-- Reviews table indexes
CREATE INDEX idx_reviews_recipe ON reviews(RecipeId);
CREATE INDEX idx_reviews_author ON reviews(AuthorId);
CREATE INDEX idx_reviews_date ON reviews(DateModified DESC);

-- Review likes table indexes
CREATE INDEX idx_review_likes_review ON review_likes(ReviewId);
CREATE INDEX idx_review_likes_author ON review_likes(AuthorId);

-- Recipe ingredients table indexes
CREATE INDEX idx_recipe_ingredients_recipe ON recipe_ingredients(RecipeId);

