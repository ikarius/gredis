package gredis

import java.util.zip.CRC32

/**
 * <p>Gredis main class.
 * User: fred
 * Date: 1 févr. 2010
 * Time: 08:59:34
 */
final class Gredis {

  private Socket socket;

  boolean pipelining  = false
  boolean sharding    = false

  int serverCount = 1

  def pipeline = []
  
  static final CRC32 = new CRC32()

  static final def BULK_COMMANDS = ['set', 'keys', 'lset', 'rpush', 'lpush', 'sadd', 'srem', 'sismember', 'smove', 'getset']
  static final def MULTI_BULK_COMMANDS = ['blpop','brpop']
  static final def STATUS_COMMANDS = ['rename','flushdb','flushall','mset','lpush']
  static final def INTEGER_COMMANDS = ['exists','delete','renamenx','dbsize','expire']
  static final def BOOLEAN_RESPONSES = ['exists', 'sadd', 'sismember', 'sinterstore', 'sunionstore', 'renamenx', 'setnx']

  static final def OK = 'OK'

  static final int DEFAULT_PORT = 6379
  
  static final int MAX_IN_PIPE  = 2048

  static final String TERM = '\r\n'


  Key newKey(String pKeyName, def value = null) {
    new gredis.Key(this, pKeyName, value)
  }

  List newList(String name, def value = []) {
    new gredis.List(this, name, value)
  }

  Set newSet(String pKeyName) {
    new gredis.Set(this, pKeyName)
  }

  Sort newSort(String pKeyName) {
    new gredis.Sort(this, pKeyName)
  }

  def booleanResponse(Integer resp) {
    resp == 1
  }


  // Categorize :)
  // Anyhow sucks ...
  // Readers consumes all data in buffered streams :(
  static String readline(Socket s) {
    assert s;

    InputStream i = s.inputStream
    String result = ""

    char prev = i.read()

    // really sucks
    if (prev > 0) {
      byte r
      while( (r = i.read()) != 0x0a && prev != 0x0d) {
        result += ((char)prev);
        prev = r;
      }
    }

    result
  }

  def connect(String pHost = "localhost", int pPort = DEFAULT_PORT) {
    socket = new Socket(pHost, pPort)
    this
  }

  def rawCall(String command, Object... args) {

    if (!socket || socket.closed)
      throw new RuntimeException("Not connected")

    // TODO check pipeline

    command = command.toLowerCase()

    def raw

    if (MULTI_BULK_COMMANDS.contains(command)) {
      //TODO
    }
    else
      if (BULK_COMMANDS.contains(command)) {
        raw = command + ' ' + args[0..-2].join(' ') + ' ' + args[-1].toString().length() + TERM + args[-1] + TERM
      }
      else {
        raw =  command + ' ' + args.join(' ') + TERM
      }

    // Send data ...
    socket << raw

    // ... process response
    def rep = processResponse()     // Consumes

    INTEGER_COMMANDS.contains(command) && BOOLEAN_RESPONSES.contains(command) ? booleanResponse(rep) : rep
  }


  def processResponse() {
    assert socket

    char first = socket.inputStream.read()

    switch (first) {

    // -- Status code reply
      case '+':
        use(Gredis) {
          socket.readline()
        }
        break

    // -- Integer reply
      case ':':
        use(Gredis) {
          Integer.parseInt socket.readline()
        }
        break

    // Bulk reply command
      case '$':
        use(Gredis) {
          int size = Integer.parseInt(socket.readline())

          if (size < 0)
            return null;

          byte[] data = new byte[size]    // TODO stream on large values

          socket.inputStream.read(data)
          socket.inputStream.skip(2) // Trailing CRLF

          new String(data)
        }
        break

    // -- Multibulk replies
      case '*':
        use(Gredis) {
          int size = Integer.parseInt(socket.readline())
          if (size < 0)
            return null
          def result = []
          size.times {
            result << processResponse()
          }
          result
        }
        break


    // -- Errors
      case '-':
        use(Gredis) {
          throw new Exception(socket.readline())
        }
        break

      default: break
    }
  }

  /* -- Shortcuts / Commons -- */

  def keys(String pattern) {
    rawCall('keys', pattern)
  }

  def delete(String... keys) {
    rawCall('del', keys)
  }

  boolean exists(String key) {
    rawCall('exists', key)
  }

  def getAt(String key) {
    rawCall('get', key)
  }

  def putAt(String key, def value) {
    rawCall('set', key, value)
  }

  def mget(String... keys) {
    rawCall('MGET', keys)
  }

  def type(String key) {
    rawCall('type', key)
  }

  String randomKey() {
    rawCall('randomkey')
  }

  boolean rename(String oldKey, String newKey, boolean nx = false) {
    nx ? rawCall('renamenx', oldKey, newKey) :OK == rawCall('rename', oldKey, newKey)
  }

  def dbsize() {
    rawCall('dbsize')
  }

  def selectdb(int db) {
    rawCall('select', db)
  }

  boolean expire(String key, int seconds) {
    rawCall('expire', key, seconds)
  }

  boolean expireAt(String key, long unixTime) {
    rawCall('expireat', key, unixTime)
  }

  int ttl(String key) {
    rawCall('ttl', key)
  }

  boolean move(String key, int destDb) {
    'OK' == rawCall('move', key, destDb)
  }

  def flushDb(int db) {
    rawCall('flushdb', db)
    this
  }

  def flushAll() {
    rawCall('flushall')
    this
  }

  def close() {
    rawCall('QUIT')
    if (!socket.closed)
      socket.close()
    this
  }

  // Use with caution
  def shutdown() {
    rawCall('SHUTDOWN')
    this
  }

  boolean auth(String pass) {
    'OK' == rawCall('AUTH', pass)
  }

  def info() {
    def infos = [:]
    rawCall('INFO').split(TERM).each {
      def ch = it.split(':')
      infos.(ch[0]) = ch[1]
    }
    infos
  }

  boolean save(background=false) {
    OK == rawCall(background ? 'BGSAVE' : 'SAVE')
  }

  boolean slaveOf(String host, int port = DEFAULT_PORT) {
    OK == rawCall('SLAVEOF', host, port)
  }

  boolean rewriteAOF() {
    OK == rawCall('BGREWRITEAOF')
  }

  boolean master() {
    OK == rawCall('SLAVEOF NO ONE')
  }

  // -- Sharding

  // Simple one
  int serverForKey(String key) {
    assert key
    CRC32.update(key.bytes)
    CRC32.getValue() % serverCount
  }


}
