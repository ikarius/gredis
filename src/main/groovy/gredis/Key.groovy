package gredis

/**
 * <p>Groovy implementation of a redis key.
 * User: fred
 * Date: 1 févr. 2010
 * Time: 13:31:18
 */
final class Key extends AbstractElement {

  Key(Gredis pGredis, String pKeyName, def value = null) {
    assert pGredis
    keyName = pKeyName
    g  = pGredis

    if (value != null) {
      set(value)
    }
    this
  }

  def getValue() {
    get()
  }

  def get() {
    assert g
    g.rawCall('get', keyName)
  }

  void setValue(def value) {
    set(value)
  }

  void set(def value, boolean nx = false) {
    assert g
    g.rawCall(nx ? 'setnx' : 'set', keyName, value)
  }

  def getSet(def value) {
    assert g
    g.rawCall('getset', keyName, value)
  }


  def String toString() {
    keyName
  }

  def incr() {
    assert g
    g.rawCall("incr", keyName)
  }

  def incr(int incr) {
    assert g
    g.rawCall('incrby', keyName, incr)
  }

  def decr() {
    assert g
    g.rawCall("decr", keyName)
  }

  long decr(int decr) {
    assert g
    g.rawCall('decrby', keyName, decr)
  }

  Key leftShift(def value) {
    assert g
    setValue(value)
    this
  }

  // Shortcuts

  /*
  def next() {
    assert g
    incr()
  }

  def previous() {
    assert g
    decr()
  }

  def plus(int value) {
    assert g
    incr(value)
  }

  def minus(int value) {
    assert g
    decr(value)
  }
  */
}