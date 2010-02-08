package gredis

/**
 * Created by IntelliJ IDEA.
 * User: fred
 * Date: 1 févr. 2010
 * Time: 14:52:39
 * To change this template use File | Settings | File Templates.
 */
final class List {

  String name
  def value
  Gredis g

  def List(Gredis g, String name, def value) {
    this.name = name;
    this.value = value;
    this.g = g;
  }

  List delete() {
    assert g
    g.rawCall('del', name)
    this
  }

  List leftShift(def elt) {
    push(elt, true)
  }

  List push(def elt, boolean tail = false) {
    assert g
    g.rawCall(tail ? 'rpush' : 'lpush', name, elt)
    this
  }

  String pop(boolean tail = false) {
    assert g
    g.rawCall(tail ? 'rpop' : 'lpop', name)
  }

  List putAt(int index, def elt) {
    assert g
    g.rawCall('lset', name, index, elt)

    this
  }

  def getAt(IntRange range) {
    assert g
    g.rawCall('lrange', name, range.reverse ? range.to : range.from, range.reverse ? range.from : range.to)
  }

  def getAt(int index) {
    assert g
    g.rawCall('lindex', name, index)
  }

  def all() {
    assert g
    this[0..-1]
  }

  int size() {
    assert g
    g.rawCall('llen', name)
  }

  def trim(IntRange range, boolean getTrimmedList = false) {
    assert g
    def result = g.rawCall('ltrim', name, range.reverse ? range.to : range.from, range.reverse ? range.from : range.to)
    if (getTrimmedList)
      result = this[0..-1]
    result
  }

}
