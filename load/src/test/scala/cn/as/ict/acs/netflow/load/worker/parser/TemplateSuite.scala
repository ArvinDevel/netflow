package cn.as.ict.acs.netflow.load.worker.parser

import java.util.concurrent.ConcurrentHashMap

import org.scalatest.{Matchers, FunSuite}

import cn.ac.ict.acs.netflow.load.worker.parser.TemplateKey
import cn.ac.ict.acs.netflow.util.IPUtils


case class TemplateKey2(routerIp: Array[Byte], templateId: Int)

class TemplateSuite extends FunSuite with Matchers {

  test("TemplateKey1 hash & equals") {
    import IPUtils._

    val hm = new ConcurrentHashMap[TemplateKey, Int]
    val key1 = TemplateKey(fromString("192.168.1.1"), 257)
    hm.put(key1, 5)
    val key2 = TemplateKey(fromString("192.168.1.1"), 257)
    assert(hm.get(key2) === 5)
  }

}
