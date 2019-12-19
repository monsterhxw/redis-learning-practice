import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.ZParams.Aggregate;

/**
 * 文章信息的散列 hash, hash-key = article:92617, sub-key_1 = title value_1 = Go to statement considered harmful ...
 *
 * 根据发布时间排序文章的有序集合 zset, key = time: member_1 = article:100408, score_1 = 1332065417.47 ...
 *
 * 根据评分排序文章的有序集合 zset, key = score: member_1 = article:100408, score_1 = 1332174713.47 ...
 *
 * 每篇文章记录的生成一个记录已投票用户的名单的集合 set, set-key = voted:100408 item_1 = user:234487  ...
 *
 * @author 黄学维
 */
public class Chapter01 {

    /**
     * 1 day = 86400 seconds
     */
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;

    /**
     * 86400 seconds / 200 = 432
     */
    private static final int VOTE_SCORE = 432;

    /**
     * 文章每页的大小值
     */
    private static final int ARTICLES_PER_PAGE = 25;

    public static void main(String[] args) {
        new Chapter01().run();
    }

    public void run() {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        String articleId = postArticle(
            conn, "username", "A title", "http://www.google.com");
        System.out.println("We posted a new article with id: " + articleId);
        System.out.println("Its HASH looks like:");
        Map<String, String> articleData = conn.hgetAll("article:" + articleId);
        for (Map.Entry<String, String> entry : articleData.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        System.out.println();

        articleVote(conn, "other_user", "article:" + articleId);
        String votes = conn.hget("article:" + articleId, "votes");
        System.out.println("We voted for the article, it now has votes: " + votes);
        assert Integer.parseInt(votes) > 1;

        System.out.println("The currently highest-scoring articles are:");
        List<Map<String, String>> articles = getArticles(conn, 1);
        printArticles(articles);
        assert articles.size() >= 1;

        addGroups(conn, articleId, new String[]{"new-group"}, null);
        System.out.println("We added the article to a new group, other articles include:");
        articles = getGroupArticles(conn, "new-group", 1);
        printArticles(articles);
        assert articles.size() >= 1;
    }

    /**
     * 文章投票
     */
    public void articleVote(Jedis jedis, String user, String article) {
        // 计算文章的投票截止时间
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;

        // 检查是否还可以对文章进行投票
        // 虽然使用散列也可以获取文章的发布时间，但有序集合返回的文章发布时间为浮点数，可以不进行转换直接使用
        if (jedis.zscore("time:", article) < cutoff) {
            return;
        }

        // 从 article:id 标识符（identifier）里面取出文章的 id
        String articleId = article.substring(article.indexOf(':') + 1);

        // 判断用户是否已投票
        // 如果用户是第一次为这篇文章投票，那么增加这篇文章的投票数量和评分
        if (jedis.sadd("voted:" + articleId, user) == 1) {
            // 使用有序集合的自增命令 zincrby，为 article:id 增加 432 分
            jedis.zincrby("score:", VOTE_SCORE, article);
            // 使用 hash 的自增命令 hincrby，增加文章信息的散列中的 votes 字段的值
            jedis.hincrBy(article, "votes", 1);
        }
    }

    /**
     * 文章发布
     */
    public String postArticle(Jedis jedis, String user, String title, String link) {
        // 生成一个新的文章 id
        // 使用一个 redis 的计数器（counter）执行 incr 命令完成
        String articleId = String.valueOf(jedis.incr("article:"));

        // 生成文章记录已投票用户的名单的集合的 set-key
        String voted = "voted:" + articleId;
        // 将发布文章的用户添加到文章的已投票用户名单里面
        jedis.sadd(voted, user);
        // 设置这个名单的过期时间为一周
        jedis.expire(voted, ONE_WEEK_IN_SECONDS);

        // 文章发布时间
        long now = System.currentTimeMillis() / 1000;
        // hash-key
        String article = "article:" + articleId;
        // 文章信息 map
        Map<String, String> articleInfo = new HashMap<>(8);
        articleInfo.put("title", title);
        articleInfo.put("link", link);
        articleInfo.put("poster", user);
        articleInfo.put("time", String.valueOf(now));
        articleInfo.put("votes", "1");
        // 将文章信息存储到一个散列里面
        jedis.hmset(article, articleInfo);

        // 将文章添加到根据发布时间排序的有序集合
        jedis.zadd("score:", now + VOTE_SCORE, article);
        // 根据评分排序的有序集合里面
        jedis.zadd("time:", now, article);

        return articleId;
    }

    public List<Map<String, String>> getArticles(Jedis jedis, int page) {
        return getArticles(jedis, page, "score:");
    }

    private List<Map<String, String>> getArticles(Jedis jedis, int page, String soredSetKey) {
        // 设置获取文章的起始索引和结束索引
        int star = (page - 1) * ARTICLES_PER_PAGE;
        int end = star + ARTICLES_PER_PAGE - 1;

        // 获取多个文章 id
        Set<String> articleIds = jedis.zrevrange(soredSetKey, star, end);

        List<Map<String, String>> articles = new ArrayList<>();

        // 根据文章 id 获取文章的详细信息
        for (String articleId : articleIds) {
            Map<String, String> articleInfo = jedis.hgetAll(articleId);
            articleInfo.put("id", articleId);
            articles.add(articleInfo);
        }

        return articles;
    }

    public void addGroups(Jedis jedis, String articleId, String[] toAdd, String[] toRemove) {
        // 构建存储文章信息的键名
        String article = "article:" + articleId;

        // 将文章添加到它所属的群组里面
        for (String group : toAdd) {
            jedis.sadd("group:" + group, article);
        }

        // 从群组里面移除文章
        if (null != toRemove && 0 < toRemove.length) {
            for (String group : toRemove) {
                jedis.srem("group:" + group, article);
            }
        }
    }

    public List<Map<String, String>> getGroupArticles(Jedis jedis, String group, int page) {
        return getGroupArticles(jedis, group, page, "score:");
    }

    private List<Map<String, String>> getGroupArticles(Jedis jedis, String group, int page, String sortedSetKey) {
        // 为每个群组的每种排列顺序都创建一个键
        String key = sortedSetKey + group;

        // 检查是否有已缓存的排序结果，如果没有的话就现在进行排序
        if (!jedis.exists(key)) {
            ZParams zParams = new ZParams().aggregate(Aggregate.MAX);
            jedis.zinterstore(key, zParams, "group:" + group, sortedSetKey);
            // 让 redis 在 60 秒之后自动删除这个有序集合
            jedis.expire(key, 60);
        }

        // 调用之前定义的 getArticles() 函数，来进行分页并获取文章数据
        return getArticles(jedis, page, key);
    }

    private void printArticles(List<Map<String, String>> articles) {
        articles.forEach(article -> {
            System.out.println("    id: " + article.get("id"));
            for (Entry<String, String> entry : article.entrySet()) {
                if (entry.getKey().equals("id")) {
                    continue;
                }
                System.out.println("    " + entry.getKey() + ": " + entry.getValue());
            }
        });
    }
}
