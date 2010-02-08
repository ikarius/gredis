package gredis

/**
 * <p>Common attributes and methods.
 * User: fred
 * Date: 4 févr. 2010
 * Time: 09:55:50
 */
abstract class AbstractElement<T> {

  protected String keyName
  protected Gredis g

  T delete() {
    assert g
    g.rawCall('del', keyName)
    this
  }

  boolean exists() {
    assert g
    g.exists(keyName)
  }
}
