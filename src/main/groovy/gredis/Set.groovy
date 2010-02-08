package gredis

/**
 * <p>Groovy implementation of a Redis Set.
 * User: fred
 * Date: 4 févr. 2010
 * Time: 09:48:37
 */
final class Set extends AbstractElement {

  Set(Gredis pGredis, String pKeyname) {
    assert pGredis
    keyName = pKeyname
    g = pGredis
  }

  boolean add(def member) {
    assert g
    g.rawCall('sadd', keyName, member)
  }

  Set leftShift(def member) {
    assert g
    add(member) // don't care about result, return this for chaining
    this
  }

  Set remove(def member) {
    assert g
    g.rawCall('srem', member)
    this
  }

  boolean contains(def member) {
    assert g
    g.rawCall('sismember', keyName, member)
  }

  int size() {
    assert g
    g.rawCall('scard', keyName)
  }

  boolean move(def member, String toKey) {
    assert g
    g.rawCall('smove', keyName, toKey, member)
  }

  def pop() {
    assert g
    random(true)
  }

  def all() {
    assert g
    g.rawCall('smembers', keyName)
  }

  def random(boolean remove = false) {
    assert g
    g.rawCall(remove ? 'spop' : 'srandmember', keyName)
  }

  def inter(Set... otherSets) {
    assert g
    def p = [] << this
    p.addAll(otherSets as java.util.List)

    Set.inter(g, false, p as Set[])
  }

  def union(Set... otherSets) {
    assert g
    def p = [] << this
    p.addAll(otherSets as java.util.List)

    Set.union(g, false, p as Set[])
  }

  def diff(Set... otherSets) {
    assert g
    def p = [] << this
    p.addAll(otherSets as java.util.List)

    Set.diff(g, false, p as Set[])
  }

  def interStore(Set... otherSets) {
    assert g
    def p = [] << this
    p.addAll(otherSets as java.util.List)

    Set.inter(g, true, p as Set[])
  }

  def unionStore(Set... otherSets) {
    assert g
    def p = [] << this
    p.addAll(otherSets as java.util.List)

    Set.union(g, true, p as Set[])
  }

  def diffStore(Set... otherSets) {
    assert g
    def p = [] << this
    p.addAll(otherSets as java.util.List)

    Set.diff(g, true, p as Set[])
  }

  static def inter(Gredis g, boolean store, Set... sets) {
    assert g
    g.rawCall(store ? 'sinterstore' : 'sinter', sets.keyName as String[])
  }

  static def union(Gredis g, boolean store, Set... sets) {
    assert g
    g.rawCall(store ? 'sunionstore' : 'sunion', sets.keyName as String[])
  }

  static def diff(Gredis g, boolean store, Set... sets) {
    assert g
    g.rawCall(store ? 'sdiffstore' : 'sdiff', sets.keyName as String[])
  }

}
