package gredis

/**
 * <p>Common attributes and methods.
 * User: fred
 * Date: 4 févr. 2010
 * Time: 09:55:50
 */
abstract class AbstractElement {

  protected String keyName
  protected Gredis g

  def delete() {
    assert g
    g.rawCall('del', keyName)
    this
  }

  boolean exists() {
    assert g
    g.exists(keyName)
  }

  int ttl() {
    assert g
    g.rawCall('ttl', keyName)
  }

  boolean expire(int seconds) {
    assert g
    g.rawCall('expire', keyName, seconds)
  }

  boolean expireAt(long unixTime) {
    assert g
    g.rawCall('expireat', keyName, unixTime)
  }


}
