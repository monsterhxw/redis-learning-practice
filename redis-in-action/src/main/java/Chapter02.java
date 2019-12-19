import com.google.gson.Gson;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

/**
 * @author 黄学维
 */
public class Chapter02 {

    public static void main(String[] args) throws InterruptedException {
        new Chapter02().run();
    }

    public void run()
        throws InterruptedException {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        testLoginCookies(conn);
        testShoppingCartCookies(conn);
        testCacheRows(conn);
        testCacheRequest(conn);
    }

    public void testLoginCookies(Jedis conn) throws InterruptedException {
        System.out.println("\n----- testLoginCookies -----");
        String token = UUID.randomUUID().toString();

        updateToken(conn, token, "username", "itemX");
        System.out.println("We just logged-in/updated token: " + token);
        System.out.println("For user: 'username'");
        System.out.println();

        System.out.println("What username do we get when we look-up that token?");
        String r = checkToken(conn, token);
        System.out.println(r);
        System.out.println();
        assert r != null;

        System.out.println("Let's drop the maximum number of cookies to 0 to clean them out");
        System.out.println("We will start a thread to do the cleaning, while we stop it later");

        CleanSessionsThread thread = new CleanSessionsThread(0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The clean sessions thread is still alive?!?");
        }

        long s = conn.hlen("login:");
        System.out.println("The current number of sessions still available is: " + s);
        assert s == 0;
    }

    public void testShoppingCartCookies(Jedis conn) throws InterruptedException {
        System.out.println("\n----- testShopppingCartCookies -----");
        String token = UUID.randomUUID().toString();

        System.out.println("We'll refresh our session...");
        updateToken(conn, token, "username", "itemX");
        System.out.println("And add an item to the shopping cart");
        addToCart(conn, token, "itemY", 3);
        Map<String, String> r = conn.hgetAll("cart:" + token);
        System.out.println("Our shopping cart currently has:");
        for (Map.Entry<String, String> entry : r.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        System.out.println();

        assert r.size() >= 1;

        System.out.println("Let's clean out our sessions and carts");
        CleanFullSessionsThread thread = new CleanFullSessionsThread(0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The clean sessions thread is still alive?!?");
        }

        r = conn.hgetAll("cart:" + token);
        System.out.println("Our shopping cart now contains:");
        for (Map.Entry<String, String> entry : r.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() == 0;
    }

    public void testCacheRows(Jedis conn) throws InterruptedException {
        System.out.println("\n----- testCacheRows -----");
        System.out.println("First, let's schedule caching of itemX every 5 seconds");
        scheduleRowCache(conn, "itemX", 5);
        System.out.println("Our schedule looks like:");
        Set<Tuple> s = conn.zrangeWithScores("schedule:", 0, -1);
        for (Tuple tuple : s) {
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert s.size() != 0;

        System.out.println("We'll start a caching thread that will cache the data...");

        CacheRowsThread thread = new CacheRowsThread();
        thread.start();

        Thread.sleep(1000);
        System.out.println("Our cached data looks like:");
        String r = conn.get("inv:itemX");
        System.out.println(r);
        assert r != null;
        System.out.println();

        System.out.println("We'll check again in 5 seconds...");
        Thread.sleep(5000);
        System.out.println("Notice that the data has changed...");
        String r2 = conn.get("inv:itemX");
        System.out.println(r2);
        System.out.println();
        assert r2 != null;
        assert !r.equals(r2);

        System.out.println("Let's force un-caching");
        scheduleRowCache(conn, "itemX", -1);
        Thread.sleep(1000);
        r = conn.get("inv:itemX");
        System.out.println("The cache was cleared? " + (r == null));
        assert r == null;

        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The database caching thread is still alive?!?");
        }
    }

    public void testCacheRequest(Jedis conn) {
        System.out.println("\n----- testCacheRequest -----");
        String token = UUID.randomUUID().toString();

        Callback callback = new Callback() {
            @Override
            public String call(String request) {
                return "content for " + request;
            }
        };

        updateToken(conn, token, "username", "itemX");
        String url = "http://test.com/?item=itemX";
        System.out.println("We are going to cache a simple request against " + url);
        String result = cacheRequest(conn, url, callback);
        System.out.println("We got initial content:\n" + result);
        System.out.println();

        assert result != null;

        System.out.println("To test that we've cached the request, we'll pass a bad callback");
        String result2 = cacheRequest(conn, url, null);
        System.out.println("We ended up getting the same response!\n" + result2);

        assert result.equals(result2);

        assert !canCache(conn, "http://test.com/");
        assert !canCache(conn, "http://test.com/?item=itemX&_=1234536");
    }

    /**
     * 尝试获取并返回令牌对应的用户
     */
    public String checkToken(Jedis jedis, String token) {
        return jedis.hget("login:", token);
    }

    public void updateToken(Jedis jedis, String token, String user, String item) {
        // 获取当前时间戳
        long timestamp = System.currentTimeMillis() / 1000;

        // 维持令牌与已登录用户之间的映射
        jedis.hset("login:", token, user);

        // 记录令牌最后一次出现的时间
        jedis.zadd("recent:", timestamp, token);

        if (null != item) {
            // 记录用户浏览过的商品
            jedis.zadd("viewed:" + token, timestamp, item);
            // 移除旧的记录，只保留用户最近浏览过的 25 个商品
            jedis.zremrangeByRank("viewed:" + token, 0, -26);

            jedis.zincrby("viewed:", -1, item);
        }
    }

    public class CleanSessionsThread extends Thread {

        private Jedis jedis;

        private int limit;

        private boolean quit;

        public CleanSessionsThread(int limit) {
            this.jedis = new Jedis("localhost");
            this.jedis.select(15);
            this.limit = limit;
        }

        public void quit() {
            this.quit = true;
        }

        @Override
        public void run() {
            while (!quit) {
                // 找出目前已有令牌的数量
                Long size = jedis.zcard("recent:");

                // 若令牌数量未超过限制，休眠，并在之后重新检查
                if (size <= limit) {
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                // 获取需要移除的令牌 id
                long endIndex = Math.min(size - limit, 100);
                Set<String> tokenSet = jedis.zrange("recent:", 0, endIndex - 1);

                // 为那些将要被删除的令牌构建键名
                String[] tokens = tokenSet.toArray(new String[tokenSet.size()]);
                List<String> sessionKeys = new ArrayList<>();
                for (String token : tokens) {
                    sessionKeys.add("viewed:" + token);
                }

                // 移除最久的哪些令牌
                jedis.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                // 从记录用户登陆信息的 hash 中移除被删除令牌对应用户的信息
                jedis.hdel("login:", tokens);
                // 清理这些用户最近浏览商品记录的有序集合
                jedis.zrem("recent:", tokens);
            }
        }
    }

    public void addToCart(Jedis jedis, String token, String item, int count) {
        if (count <= 0) {
            jedis.hdel("cart:" + token, item);
        } else {
            jedis.hset("cart:" + token, item, String.valueOf(count));
        }
    }

    public class CleanFullSessionsThread extends Thread {

        private Jedis jedis;

        private int limit;

        private boolean quit;

        public CleanFullSessionsThread(int limit) {
            this.jedis = new Jedis("localhost");
            this.jedis.select(15);
            this.limit = limit;
        }

        public void quit() {
            this.quit = true;
        }

        @Override
        public void run() {
            while (!quit) {
                // 找出目前已有令牌的数量
                Long size = jedis.zcard("recent:");

                // 若令牌数量未超过限制，休眠，并在之后重新检查
                if (size <= limit) {
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                // 获取需要移除的令牌 id
                long endIndex = Math.min(size - limit, 100);
                Set<String> tokenSet = jedis.zrange("recent:", 0, endIndex - 1);

                // 为那些将要被删除的令牌构建键名
                String[] tokens = tokenSet.toArray(new String[tokenSet.size()]);
                List<String> sessionKeys = new ArrayList<>();
                for (String token : tokens) {
                    sessionKeys.add("viewed:" + token);
                    // 新增的代码用于删除旧会话对应用户的购物车
                    sessionKeys.add("cart:" + token);
                }

                // 移除最久的哪些令牌
                jedis.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                // 从记录用户登陆信息的 hash 中移除被删除令牌对应用户的信息
                jedis.hdel("login:", tokens);
                // 清理这些用户最近浏览商品记录的有序集合
                jedis.zrem("recent:", tokens);
            }
        }
    }

    public interface Callback {

        String call(String request);
    }

    public String cacheRequest(Jedis jedis, String request, Callback callback) {
        // 对于不能被缓存的请求，直接调用回调函数
        if (!canCache(jedis, request)) {
            return callback != null ? callback.call(request) : null;
        }

        // 将请求转换成一个简单字符串键，方便之后进行查找
        String pageKey = "cache:" + hashRequest(request);
        // 尝试查找被缓存的页面
        String content = jedis.get(pageKey);

        // 如果页面还未被缓存，那么生成页面
        if (content == null && callback != null) {
            content = callback.call(request);
            // 将新生成的页面放到缓存里面
            jedis.setex(pageKey, 300, content);
        }

        return content;
    }

    public boolean canCache(Jedis jedis, String request) {
        try {
            URL url = new URL(request);
            Map<String, String> params = new HashMap<>();
            if (null != url.getQuery()) {
                for (String param : url.getQuery().split("&")) {
                    String[] pair = param.split("=", 2);
                    params.put(pair[0], param.length() == 2 ? pair[1] : null);
                }
            }
            String itemId = extractItemId(params);
            if (itemId == null || isDynamic(params)) {
                return false;
            }
            Long rank = jedis.zrank("viewed:", itemId);
            return rank != null && rank < 10000;
        } catch (MalformedURLException e) {
            return false;
        }
    }

    public boolean isDynamic(Map<String, String> params) {
        return params.containsKey("_");
    }

    public String extractItemId(Map<String, String> params) {
        return params.get("item");
    }

    public String hashRequest(String request) {
        return String.valueOf(request.hashCode());
    }

    public void scheduleRowCache(Jedis jedis, String rowId, int delay) {
        // 先设置数据行的延迟值
        jedis.zadd("delay:", delay, rowId);
        // 立即对需要缓存的数据行进行调度
        jedis.zadd("schedule:", System.currentTimeMillis() / 1000, rowId);
    }

    public class CacheRowsThread extends Thread {

        private Jedis jedis;
        private boolean quit;

        public CacheRowsThread() {
            this.jedis = new Jedis("localhost");
            this.jedis.select(15);
        }

        public void quit() {
            quit = true;
        }

        @Override
        public void run() {
            Gson gson = new Gson();
            while (!quit) {
                // 尝试获取下一个需要被缓存的数据行以及该行的调度时间戳，命令会返回一个包含零个或者一个 Tuple 类的列表
                Set<Tuple> range = jedis.zrangeWithScores("schedule:", 0, 0);
                Tuple next = range.size() > 0 ? range.iterator().next() : null;

                long now = System.currentTimeMillis() / 1000;

                // 暂时没有行需要被缓存，休眠 50 毫秒后重试
                if (next == null || next.getScore() > now) {
                    try {
                        sleep(50);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                String rowId = next.getElement();

                // 提前获取下一次调度的延迟时间
                double delay = jedis.zscore("delay:", rowId);

                // 如果延迟值小于或等于 0
                if (delay <= 0) {
                    // 不必再缓存这个行，将它从缓存中移除
                    jedis.zrem("delay:", rowId);
                    jedis.zrem("schedule:", rowId);
                    jedis.del("inv:" + rowId);
                    continue;
                }

                // 读取数据行
                Inventory row = Inventory.get(rowId);

                //更新调度时间并设置缓存值
                jedis.zadd("schedule:", now + delay, rowId);
                jedis.set("inv:" + rowId, gson.toJson(row));
            }
        }
    }

    public static class Inventory {

        private String id;
        private String data;
        private long time;

        private Inventory(String id) {
            this.id = id;
            this.data = "data to cache...";
            this.time = System.currentTimeMillis() / 1000;
        }

        public static Inventory get(String id) {
            return new Inventory(id);
        }
    }
}
