package gredis

/**
 * <p>Sort class: use on sets and lists.
 * User: fred
 * Date: 7 févr. 2010
 * Time: 15:54:39
 */
final class Sort {

  String keyName
  Gredis g

  def sort
  def gets 

  static final String ALPHA = 'ALPHA'
  static final String DESC  = 'DESC'
  static final String ASC   = 'ASC'
  static final String LIMIT = 'LIMIT'
  static final String BY    = 'BY'

  Sort(Gredis pGredis, String pKeyName) {
    assert pGredis
    g = pGredis
    keyName = pKeyName
    sort = new String[6]
    gets = [] as java.util.Set
  }

  def asc() {
    sort[3] = ASC
    this
  }

  def desc() {
    sort[3] = DESC
    this
  }

  def limit(int from, int to) {
    assert from > -1
    assert to > 0
    sort[1] = LIMIT + ' ' + from + ' ' + to
    this
  }

  def by(String pattern) {
    assert pattern
    sort[0] = 'BY ' + pattern
    this
  }

  def alpha() {
    sort[4] = ALPHA
    this
  }

  def clear() {
    sort = new String[6]
    gets.clear()
    this
  }

  def store(String key) {
    if (key)
      sort[5] = key
    this
  }

  def get(String pattern) {
    assert pattern
    gets << 'GET ' + pattern
    if (gets)
      sort[2] = gets.join(' ')
    this
  }

  def apply() {
    assert g
    g.rawCall('SORT', keyName, toString() )
  }

  def String toString() {
    sort.findAll { it != null }.join(' ')
  }

}
