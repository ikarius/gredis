package gredis

import java.security.MessageDigest

/**
 * <p>Sharding for Gredis.
 * <p>Implementation of consistent hashing provided by: Tom White
 * http://weblogs.java.net/blog/tomwhite/archive/2007/11/consistent_hash.html
 * <p><b>ALPHA CODE</b> (you're warned)
 * User: fred
 * Date: 9 févr. 2010
 * Time: 15:40:55
 */
final class Sharding {

  static final int NB_NODES = 256     // Number of nodes (points) per server

  def sockets    = [:]

  SortedMap circle = new TreeMap()

  def addServer(String host, int port = 6379) {
    def sh = host + ':' + port

    sockets."$sh" = new Socket(host, port)

    NB_NODES.times {
      circle.(hashKey(host+port+it)) = sh
    }
  }

  def getServerForKey(String key) {
    if (circle) {
      def h = '' + hashKey(key)
      if (!(h in circle.keySet())) {
        SortedMap tailMap = circle.tailMap(h);
        h = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
      }
      circle.get(h);
    }
  }

  static int hashKey(String key) {
    MessageDigest digest = MessageDigest.getInstance("MD5")
    digest.update(key.bytes);
    new BigInteger(1, digest.digest()).intValue()
  }

}
