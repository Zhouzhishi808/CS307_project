// ----------------- 新增/调整导入 START -----------------
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.List;
// ----------------- 新增/调整导入 END -----------------

// ...existing code...

{
  // 替换原来将 ResultSet 映射为 FeedItem 的那段代码为下面实现：
  // 注意：只替换映射部分（jdbcTemplate.query(...) 的 RowMapper），其余逻辑保持不变。

  // ...existing code...
  List<FeedItem> items = jdbcTemplate.query(dataSql, paramsArray, (rs, rowNum) -> {
      FeedItem item = new FeedItem();
      item.setRecipeId(rs.getInt("RecipeId"));
      item.setName(rs.getString("Name"));
      item.setAuthorId(rs.getInt("AuthorId"));
      item.setAuthorName(rs.getString("AuthorName"));

      // 以 UTC 解析 DatePublished，避免本地时区偏移
      Calendar utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      Timestamp ts = rs.getTimestamp("DatePublished", utcCal);
      if (ts != null) {
          OffsetDateTime odt = ts.toInstant().atOffset(ZoneOffset.UTC);
          // 如果 FeedItem#setDatePublished 接受 OffsetDateTime，则直接使用：
          item.setDatePublished(odt);
          // 如果 FeedItem 需要其他类型（例如 Instant 或 ZonedDateTime），请改为：
          // item.setDatePublished(odt.toInstant());
      } else {
          item.setDatePublished(null);
      }

      // AggregatedRating 为 NULL 时返回 0.0 而不是 null
      double agg = rs.getDouble("AggregatedRating");
      if (rs.wasNull()) {
          agg = 0.0;
      }
      item.setAggregatedRating(agg);

      item.setReviewCount(rs.getInt("ReviewCount"));
      return item;
  });
  // ...existing code...
}
