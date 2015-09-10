/**
 * Copyright 2015 ICT.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.ac.ict.acs.netflow.load.worker.parquet

import scala.collection.immutable.HashMap
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api.{Binary, RecordConsumer}

import cn.ac.ict.acs.netflow.Logging
import cn.ac.ict.acs.netflow.load.worker.DataFlowSet
import cn.ac.ict.acs.netflow.load.worker.bgp.BGPRoutingTable

class PseudoFlowSetWriteSupport extends WriteSupport[DataFlowSet] with Logging {
  import ParquetSchema._

  private val schema = overallSchema
  private var consumer: RecordConsumer = null

  override def init(configuration: Configuration) = {
    new WriteSupport.WriteContext(schema, new HashMap[String, String].asJava)
  }

  override def prepareForWrite(recordConsumer: RecordConsumer) = {
    consumer = recordConsumer
  }

  case class InnerRecord(ipSrc: Array[Byte], ipDst: Array[Byte], nexthop: Array[Byte],
    inputsnmp: Long, outputsnmp: Long, inpkts: Long, inbytes: Long,
    fsw: Long, lsw: Long, srcp: Int, dstp: Int, flag: Int, pro: Int,
    srctos: Int, srcas: Long, dstas: Long, srcmask: Int, dstmask: Int)

  // the elements below generated from the following script

  // scalastyle:off
  /*
  import scala.util.Random

  val r = new Random(47)

  val ipSrc: Array[Byte] = new Array[Byte](4)
  val ipDst: Array[Byte] = new Array[Byte](4)
  val nexthop: Array[Byte] = new Array[Byte](4)

  r.nextBytes(ipSrc)
  ipSrc.mkString(", ")

  var i = 0
  while (i < 50) {
    println(s"  InnerRecord(Array[Byte](${r.nextBytes(ipSrc); ipSrc.mkString(", ")}), Array[Byte](${r.nextBytes(ipDst); ipDst.mkString(", ")}), Array[Byte](${r.nextBytes(nexthop); nexthop.mkString(", ")}), " +
      s"${r.nextLong()}L, ${r.nextLong()}L, ${r.nextLong()}L, ${r.nextLong()}L, ${r.nextLong()}L, ${r.nextLong()}L, ${r.nextInt()}, ${r.nextInt()}, ${r.nextInt()}, ${r.nextInt()}, ${r.nextInt()}, ${r.nextLong()}L, ${r.nextLong()}L, ${r.nextInt()}, ${r.nextInt()}),")
    i += 1
  }
  */

  val pseudoRecords = Seq(
    InnerRecord(Array[Byte](22, 5, 91, 102), Array[Byte](-85, 10, -20, -121), Array[Byte](74, 108, -84, 13), 2955289354441303771L, 3476817843704654257L, -8917117694134521474L, 4941259272818818752L, 4821260142127765371L, 938337042401844188L, -843035300, 865149722, -1021916256, -1916708780, -2016789463, 2897849999215005902L, 7316324942259685329L, 1072754767, -846991883),
    InnerRecord(Array[Byte](-65, 91, 25, 29), Array[Byte](-44, 22, 5, 6), Array[Byte](-83, 21, -4, -52), -6924998884410695000L, -243908593435350267L, 8927642045409361120L, 170579209766410800L, 7465016891723527973L, 5025011261803023659L, 691554276, -1004355271, -541407364, 1920737378, -1278072925, 1208921558810358757L, -4103494832693768930L, -2091036560, -1748303588),
    InnerRecord(Array[Byte](-89, 104, 86, -62), Array[Byte](34, 50, -49, -92), Array[Byte](100, -32, 44, -26), 3501888837866123916L, 5952063745670845647L, -5450608913818554203L, -7315860422829928998L, 7897551365385265717L, -1667465798672716859L, 911089923, 1910265799, 209074105, 1868533330, -550041988, -6644398060334941084L, -7369746740960866972L, -464038979, 1133511807),
    InnerRecord(Array[Byte](-30, 61, 97, 82), Array[Byte](-67, 90, 117, -59), Array[Byte](-20, -86, 105, -109), -6191244130952668704L, -8528487273298348806L, 2408420617613712128L, 6167679450490020352L, 5238952726406660118L, -4670864486318933195L, -85075115, -1567871222, -297486697, -1926385554, -185366150, 7825963136307975883L, -7713828436757809333L, 746227392, -1540847176),
    InnerRecord(Array[Byte](69, -110, -99, 62), Array[Byte](-54, 65, -48, 117), Array[Byte](71, -10, 37, 60), 2237228738573044948L, 1788642417581210309L, 1727108243069619221L, 7125817458294863276L, -4814762211230589829L, -366024740914952159L, 132392457, -524830861, -740435193, -2145699942, 1700756610, 3841626164676692156L, 1438329165838292665L, -116252807, 69592999),
    InnerRecord(Array[Byte](-97, -60, -117, 99), Array[Byte](-92, -80, 68, -55), Array[Byte](-36, 94, -77, -91), -3540351928265827338L, -7111676695848063677L, 4166409359504350445L, 4184268206388128495L, -3428094500490031374L, -868591117884234637L, 978772007, -995800869, 1553980751, -1200398477, 1939313338, -5399088840969356540L, 1508969400313453937L, -1407026845, -194292093),
    InnerRecord(Array[Byte](61, -40, 48, -8), Array[Byte](-110, -37, 41, -82), Array[Byte](-121, -127, 81, -98), 7232118468295425308L, -8976481792456329053L, -4510200041955120919L, 7895101010162822524L, -8465635458291897621L, 8522235356765497948L, 838001420, 1198182742, 1249673827, -352468238, -1468557263, -5647931965208864460L, 472909253322638467L, -942308824, -1395684015),
    InnerRecord(Array[Byte](16, 20, -38, -77), Array[Byte](-96, 101, 85, -49), Array[Byte](118, -117, -115, 21), 1937968565486823290L, 2331946426665529669L, -6112014171803040797L, 862789655194884896L, -8003608149974376682L, -2607410296060199335L, 1353473926, -1478885669, -643937365, -1184363001, -88825106, -191382516906461459L, -7495846502989670662L, 790479692, -1181553834),
    InnerRecord(Array[Byte](-19, -67, -56, 10), Array[Byte](-119, 4, 45, 76), Array[Byte](-124, -99, 78, 107), 4707556060886879460L, 3337524516042226159L, 2385184619853533079L, -2597415584606730266L, -2249001416330393851L, -6728056606699446690L, 936563251, 260558669, 18834451, -800295167, 1635969696, 1287265823259941451L, 5467712561770403692L, 964484446, -405611652),
    InnerRecord(Array[Byte](-73, 109, 54, -85), Array[Byte](-47, 40, -72, -121), Array[Byte](41, -104, -121, 100), 296553280159801481L, 3269569705817844378L, -2446665924302807794L, 1761624404197313517L, -2701346113595137934L, -1072284027743501098L, 1565034249, 1964410033, 1389234317, 549250758, -1166682820, -6541527391694986222L, 5709002386429283679L, -646225044, -1789866806),
    InnerRecord(Array[Byte](126, -72, 60, 2), Array[Byte](-126, -99, -84, -92), Array[Byte](67, 99, -9, -85), 3947688279467779000L, -1377397010723768397L, -2337165758903692508L, -6506219287285821647L, 7936799459336021404L, -6784559955903246308L, -308207302, 2132594573, -809694861, 1153910049, -957425588, -5186044517877997174L, 8224465586118388887L, -1071504946, 753574493),
    InnerRecord(Array[Byte](-49, 9, -29, -58), Array[Byte](37, 12, -48, -9), Array[Byte](67, 101, -64, -65), -7702612389941449890L, 4616456849522867651L, -300261439657119396L, -7347843767736324131L, -9069953026553802599L, 8512568830394485644L, 1860334543, -1796266037, 1097780082, 217026235, -339355193, -2685113268323782353L, -8240905579160877106L, -1007318651, -1184223618),
    InnerRecord(Array[Byte](-46, 121, 91, -104), Array[Byte](-11, -124, 92, 60), Array[Byte](125, 47, -123, 15), -1550545968385192602L, 6202946753789610966L, 5055909466654364L, 3887810038252452320L, -2172600003493638261L, -6880985797526637203L, -1439074129, 331666581, -1607421482, -528827280, 1791692955, 7928156851693820577L, 3748656116014754148L, -744928833, 751695820),
    InnerRecord(Array[Byte](-30, -40, 103, -10), Array[Byte](89, -24, -24, 115), Array[Byte](-82, 121, 122, 14), -205575978672082138L, -6621055782532096225L, 3349264634131621264L, 4448681388160221911L, 5651036129709951003L, 3243263001831619901L, -923586839, 1519492602, 137980282, 1003825181, -1025715111, -3616699467021608075L, -6121543458273210357L, 742302528, 414612633),
    InnerRecord(Array[Byte](-65, 21, -120, 126), Array[Byte](38, 38, 23, 51), Array[Byte](-9, -20, -8, 67), -2615517375699075472L, 746437415474728106L, 2661424102271702208L, 9177102290105166731L, 1269036823232413866L, 1328701697379878945L, -2068189571, 1231593320, -1840123223, 803180838, 1379640480, -2996596965662587436L, -184776557062800779L, 760661441, -1511639070),
    InnerRecord(Array[Byte](-102, 33, -44, -128), Array[Byte](65, 8, 100, -121), Array[Byte](-34, 6, -80, -55), -7872433379718262510L, 9177082184160322074L, 721833805496139417L, -3744075038028818282L, -7857117600621367274L, 7661926886533963144L, 473767674, -1061568451, 1524488617, 969358141, 932889664, -5315807537435790576L, 5087876121557218453L, 2101026177, -589814640),
    InnerRecord(Array[Byte](124, -107, 119, -114), Array[Byte](-117, 121, -1, -35), Array[Byte](19, 23, 31, 55), 7257640035424758463L, 1962838414885708997L, 4710408941888523776L, 2536805894612973038L, 8067249964390899970L, -3934204434112527282L, 584026024, -971503623, 266836183, -386634707, 1986355488, 2132938542479117207L, 9199484886821554672L, 1109913503, -2146488606),
    InnerRecord(Array[Byte](-38, 46, 99, -47), Array[Byte](74, 63, -84, -35), Array[Byte](9, 23, -21, -79), 3035664570991790001L, 1804921159994267305L, 2616147413649948195L, 531002529326603614L, -6058679848044719096L, -3817284984423259948L, -1908772069, 166585819, -396178, -1384916546, -659080842, -3392254382095560984L, -4209809699724612597L, 155094520, 1376928043),
    InnerRecord(Array[Byte](90, -51, 102, 56), Array[Byte](106, -70, 92, 44), Array[Byte](-90, 104, 125, 24), 1982272129392804504L, -5006639162985342189L, 1040459690336452778L, 305790788400451137L, 5530668325255048254L, 7104978152400232171L, -1222709927, 384870377, -299102349, -66783507, 2101235668, 2409397364279273846L, 4644364473603952901L, 1475925910, 203173975),
    InnerRecord(Array[Byte](14, -126, 33, -93), Array[Byte](63, -49, 51, -128), Array[Byte](-105, 121, 126, 25), 7410131048639366782L, -3915226275298555660L, -6717692682124735635L, 5540413533507083503L, 4124889763059434474L, 9187915334774042250L, -530167038, 1843745718, 657216100, 1186631685, 800217863, 1549375102516040613L, -7383483756646392368L, 1183398137, -1503195707),
    InnerRecord(Array[Byte](-74, -120, 80, 121), Array[Byte](-64, 85, 24, 69), Array[Byte](10, 107, -62, 126), 4268099931763370449L, 8535583749589630762L, 8964052857546370914L, -1279601005219468600L, 4509503310673861476L, -2322453001528088415L, 2142648604, 180808558, 636800914, 1227577190, 295273454, -8525998047641535430L, 6055897406345406641L, 912981957, 123740029),
    InnerRecord(Array[Byte](-63, 61, -110, 45), Array[Byte](74, -63, 19, 15), Array[Byte](-37, 65, 8, -78), 6923477957236418999L, 592027925246932819L, 9123117600149691484L, 3667891185009943481L, 603696407920786041L, -908664091068636791L, -1289248667, -2056424482, -569643434, -614729486, -130383186, -3924111051265773065L, 6830446452905333740L, 163391375, -1313323822),
    InnerRecord(Array[Byte](44, 96, 80, -65), Array[Byte](104, -42, 53, 33), Array[Byte](118, 89, 45, -44), -8348283546176094906L, 5660448069562215903L, 4515448715893684691L, 8669792375814782124L, 5374608782813848772L, 3448403522035519712L, 718404710, 75160725, 2085461368, 2022150274, 1488360928, -3461979666667099320L, -2300918542572896830L, 410835430, -186798877),
    InnerRecord(Array[Byte](111, -68, -54, -128), Array[Byte](60, -106, -67, -91), Array[Byte](1, -20, 38, 7), 5188572931152489071L, 8224272632628318845L, -4685664232645635377L, 1589710492459191888L, -5444080867331195947L, 7030840558537530069L, -103008870, -1129789085, -478777721, 936511439, 598012351, -1751811621118820330L, 2316028528711014964L, 1125138894, -1523331676),
    InnerRecord(Array[Byte](15, -50, -74, 103), Array[Byte](122, -46, -81, -73), Array[Byte](-116, 12, 23, 17), 6340303060447722121L, -2123985339117690343L, -4464011903458030682L, 5714922501056838155L, -6315964179853580754L, -2084524197553029841L, 2007033692, -355844949, 1891342070, -1477805397, -2985382, 8078687153884330314L, -7235864710972847075L, -835577026, -1437577904),
    InnerRecord(Array[Byte](-101, -121, 68, -14), Array[Byte](16, 114, -51, -14), Array[Byte](73, 124, -66, 11), -8641498894842957084L, 3824448922264558590L, -8974818479792389580L, -391600308948526303L, -3497662567858855857L, -8379456578903786863L, 995954050, 224187502, -1877722831, 732650632, -513318068, -1880412146515619194L, -5613290396916003157L, -974970520, 2907504),
    InnerRecord(Array[Byte](24, 85, -97, -121), Array[Byte](-13, 29, 28, 18), Array[Byte](49, -30, -118, -14), -7434976923060118056L, -5753578771886258716L, 741991936464233906L, 584047533145664589L, -4969296731861611158L, 8887144056702986440L, -2007584047, 1280163831, 899897105, 92817562, -923770732, 3755783884166741523L, -5430466360474057410L, 357645009, -936530459),
    InnerRecord(Array[Byte](35, 35, 4, -51), Array[Byte](-84, 38, -84, 92), Array[Byte](117, 44, 83, 45), -758603014268941548L, -7684755153607084312L, -8606202707328741030L, 3751413521654714980L, 5823212294379388797L, -6856023982213639378L, 764752039, -1468900719, -933054387, 1098659354, -2056028287, 883670138728642591L, -1454045491464263894L, 1868206209, -1428754546),
    InnerRecord(Array[Byte](92, -51, 89, 95), Array[Byte](-107, -58, -108, -108), Array[Byte](-93, -43, -8, -40), 6261364235563928146L, -7605652070839257145L, 8071738837506905459L, 2094337484319504057L, 5784711151757480675L, 8531771314242044002L, 638071183, -564166740, 1740157669, 678481009, 1250911180, 2853153535002955198L, 9032195790090257919L, 438480344, -1370314365),
    InnerRecord(Array[Byte](-46, 96, -27, -45), Array[Byte](-24, -63, -107, 115), Array[Byte](127, 123, 27, 40), -1669664073733123699L, 3730436767719227085L, -5623847832207856506L, 3262364981343950931L, 9198976611093802897L, 6121479898599713507L, -525941854, 51234652, -682987372, 1495521104, 2116842578, -4262099786042704565L, -6844644091410599875L, -605820730, 730020095),
    InnerRecord(Array[Byte](-126, 43, -20, -80), Array[Byte](15, -99, -30, 39), Array[Byte](-56, 59, -72, -104), 6556319598950535275L, -5076756770898134952L, 4236361777701121727L, -656192550325139130L, -3265116056649934172L, -3507943403354984881L, -1934119719, 992802333, -1137847349, -582356726, 1434541073, 7531789387645686039L, 2142310479117107100L, -487934346, 1418068846),
    InnerRecord(Array[Byte](53, 48, 36, 44), Array[Byte](-45, -126, 2, 40), Array[Byte](-115, -77, -69, -112), 8240153462679188612L, 7010509799062960363L, -372562623824446517L, 5382564651920031131L, 5725395596931370219L, 1809682412336553822L, -807292223, 553359667, -1283469371, 1241870763, -1163249833, 705109131444951695L, 6113206543884601679L, -1117075782, -1006208204),
    InnerRecord(Array[Byte](53, 86, -102, -124), Array[Byte](95, 79, 9, 80), Array[Byte](-62, -41, -108, -1), 2044946094625400479L, 358575797517153296L, -636200445957727306L, -4212333402746817612L, -5661340543124303993L, -3852305001532929336L, -1175850960, 275716400, 1204466359, -61138032, -1646360000, 810759684378972989L, -7701781134095763467L, -1932076888, -74028240),
    InnerRecord(Array[Byte](-79, 30, -45, 12), Array[Byte](51, 90, -99, 97), Array[Byte](40, 58, 97, -124), -1767899580803604696L, 1281479539475149735L, 1962026428584972693L, 667829150386594322L, 7186033133521944043L, 5049189308524232634L, 1505392360, -1747513762, -51718617, -1473779040, 140326133, 32309462369284687L, 3181664431891030537L, -1195850415, -92972533),
    InnerRecord(Array[Byte](-43, -35, -3, 25), Array[Byte](118, 88, 92, -108), Array[Byte](-53, 59, 54, 24), 8092495420416731237L, 5057859881648987482L, -1358942298704832775L, 7670432383299488988L, 4220909598056549547L, 3497022071466009735L, 290564180, 1131374359, 745355494, -1247909554, 1935235917, 4140279620025895047L, 6461276270866072852L, -1249097347, -1294430782),
    InnerRecord(Array[Byte](-44, -5, 30, -34), Array[Byte](77, 90, 36, 106), Array[Byte](-86, 62, 108, -49), 3941374327598288694L, -8063400076504700866L, -2298259592304179835L, 1011568222218255785L, -6123970558136572151L, 4527165352631480499L, -448874570, -57702816, 89325392, 1966608136, 1915252928, -8870077374790794804L, 4876967341947780341L, -1740753470, -583886565),
    InnerRecord(Array[Byte](116, -86, 7, -109), Array[Byte](29, 73, 111, -58), Array[Byte](61, -86, 27, 42), -6135009954711720091L, -8016410562273342735L, -6600770647565720546L, -18913823019425210L, -2122521892020147931L, -1235182847424162411L, 81262988, -1302267589, 65898936, 75955654, -1992057146, -4247760188793187673L, 6266589799791749104L, 61664389, 862795751),
    InnerRecord(Array[Byte](-23, 85, 65, 5), Array[Byte](-27, -71, 4, -48), Array[Byte](-10, -102, 66, -44), -2066637774550874417L, -3442899395475557703L, 1382666446882320260L, 3296196410443228066L, -5108334819430563031L, 2006133212557192983L, 813223659, -2080158838, 1876037158, -1285224238, 1945066270, -5302635065461102224L, 2829034506715020137L, 832374680, -704256973),
    InnerRecord(Array[Byte](-2, -13, -119, -98), Array[Byte](21, 46, -61, 123), Array[Byte](-14, -22, 100, -77), -1541998314439238932L, -726321254858305942L, 4027685939844766305L, 6428552422254748668L, -1493070137093965319L, 9024237045897795683L, 918974759, -1930890885, -398378535, -675687164, -1431860077, -7300661151948487157L, 6468572084371040882L, 1623503757, 540986842),
    InnerRecord(Array[Byte](-47, -62, 25, 96), Array[Byte](-121, 78, 125, -71), Array[Byte](110, -6, -122, -103), 2359722873301585543L, 6890928092384539680L, 5668415284318779152L, 8503593807015252996L, 263204608446649386L, 3951965401693750399L, 1091203336, 395098322, 2019139621, -1774245123, -1851793361, -4447162911474634675L, 4054972736045250209L, -637709047, 1570703135),
    InnerRecord(Array[Byte](-71, 24, -31, 43), Array[Byte](-124, 23, 70, -120), Array[Byte](-51, -56, -14, -84), 7468871311501859230L, -4929868624891349234L, -4269997704432134754L, -5667539339977490275L, 8109917882943462986L, 7769405413136093470L, -648563131, -264645227, 1715173285, 89706034, 1110195714, -1552990259404000118L, -4657754626938985288L, 1516336267, -1672438741),
    InnerRecord(Array[Byte](16, 41, -48, -47), Array[Byte](5, -94, -49, 73), Array[Byte](33, -101, 3, 71), 2307154228048116672L, 7150424353233859313L, -1325928025002579237L, -3665842271398793394L, -6130621713096433728L, -29247115312397763L, 1413611318, -338104353, 1193585633, 459630532, 1093694443, 3252764008405165794L, -5983347223574435284L, -273332343, 2145335468),
    InnerRecord(Array[Byte](-17, 37, 62, 55), Array[Byte](-115, 5, -47, 14), Array[Byte](-105, 43, -48, 17), 3465303585701562733L, 1700613420190201068L, 5822184238746542461L, 1093857116994020787L, -8085781767694554673L, 9184631961804259134L, 1438227921, -861819677, 1672440012, -1292508562, -2091483647, -1565505466710621890L, -7603322637104951913L, 349931990, -1307982363),
    InnerRecord(Array[Byte](36, 125, -32, 80), Array[Byte](67, 7, -55, -119), Array[Byte](66, 61, 96, 109), 7229074476884094776L, -7721322612123693614L, -5930320724767452010L, -3751893598807007127L, 1766870678437293186L, 5321722921787709079L, 390940266, 1936561000, -159428915, -1406794470, 287121291, -6890007172123177697L, -2377180808406387829L, 1180977313, 750476524),
    InnerRecord(Array[Byte](-24, 38, 54, -112), Array[Byte](64, -66, 11, 46), Array[Byte](13, 2, -113, 20), 9025602761985638856L, -684838898613284819L, -8153825059683536146L, -7529928327978674105L, 1739001200855335304L, -8000989201603277986L, 550798801, -1256524727, 944062771, 755940314, -766241620, -4394077599709281875L, 1084216764299328102L, -489828199, -891759629),
    InnerRecord(Array[Byte](28, -89, -62, -78), Array[Byte](49, 82, -14, -14), Array[Byte](-33, 20, 119, 68), -8627253187548299296L, -5522952936810441596L, -8724660625552114423L, 7410957068283334882L, 582514480314610501L, -2888220309987880909L, -936398904, -349881923, 723815996, -1602002089, 31839276, -1556574803885334614L, -7528818097187247338L, 160150267, 2026332861),
    InnerRecord(Array[Byte](29, -114, -51, 56), Array[Byte](-77, 80, -67, -52), Array[Byte](57, 112, -110, -112), 2549843351848344598L, -5500615358193751895L, 4589156401759618007L, 3809928322769689012L, 8499247796964736688L, -6706315245463345838L, 356064225, 1133821036, 457299740, -1118887693, 1948316169, 6442441984436842542L, -2824057533694561801L, -1498949240, -1876678206),
    InnerRecord(Array[Byte](104, -124, 71, 58), Array[Byte](23, 23, 62, 36), Array[Byte](-47, -47, 12, 113), -5668681353484319250L, -6381441893082031334L, -5000793134205414299L, -3282037170827445416L, 6336540799221905578L, 4196742706910454740L, -1912003002, -1914664028, 2076897191, -41031118, 1714128560, 15693258947719265L, 4025036841977731617L, -1993508928, -2034491257),
    InnerRecord(Array[Byte](-87, 76, 127, -108), Array[Byte](112, 73, -45, 53), Array[Byte](0, -10, 97, -10), -3593558061931966876L, -6955536798508469831L, 1504819111184213258L, -2553901493965583174L, 6031315059293875187L, -1182682365123516812L, 1386672891, 947815975, 219396232, 1292560422, 834329616, 2093612111641392009L, -2861000478194026777L, -1436196909, -633937111),
    InnerRecord(Array[Byte](-20, 57, -2, 103), Array[Byte](23, -107, 90, -89), Array[Byte](-15, 17, 20, 75), 8292361169968937523L, -5855512602419225751L, 7765232381527143132L, -5420173759238983630L, -6730379016370093324L, 5731330059965745222L, 138818964, -101432472, -225047505, 318271473, -1953965849, -5843602014159409631L, 3153883457079300703L, -397710299, 265180228))

  // scalastyle:on

  override def write(flowSet: DataFlowSet) = {

    var curRowPos: Int = flowSet.startPos + flowSet.fsHeaderLen

    val endPos: Int = flowSet.endPos
    val packetTime: Long = flowSet.packetTime
    val routerIp: Array[Byte] = flowSet.routerIp

    val rowLength = flowSet.template.rowLength

    val totalPseudoRecords = pseudoRecords.length

    var i = 0
    while (curRowPos != endPos) {

      val record = pseudoRecords(i % totalPseudoRecords)

      consumer.startMessage()

      consumer.startField("time", 0)
      consumer.addLong(packetTime)
      consumer.endField("time", 0)

      if (routerIp.length == 4) {
        consumer.startField("router_ipv4", 1)
        consumer.addBinary(Binary.fromByteArray(routerIp))
        consumer.endField("router_ipv4", 1)
      } else {
        consumer.startField("router_ipv6", 2)
        consumer.addBinary(Binary.fromByteArray(routerIp))
        consumer.endField("router_ipv6", 2)
      }

      consumer.startField("ipv4_src_addr", 10)
      val bytes0: Array[Byte] = new Array[Byte](4)
      consumer.addBinary(Binary.fromByteArray(record.ipSrc))
      consumer.endField("ipv4_src_addr", 10)

      consumer.startField("ipv4_dst_addr", 14)
      val bytes1: Array[Byte] = new Array[Byte](4)
      consumer.addBinary(Binary.fromByteArray(record.ipDst))
      consumer.endField("ipv4_dst_addr", 14)

      consumer.startField("ipv4_next_hop", 17)
      val bytes2: Array[Byte] = new Array[Byte](4)
      consumer.addBinary(Binary.fromByteArray(record.nexthop))
      consumer.endField("ipv4_next_hop", 17)

      consumer.startField("input_snmp", 12)
      consumer.addLong(record.inputsnmp)
      consumer.endField("input_snmp", 12)

      consumer.startField("output_snmp", 16)
      consumer.addLong(record.outputsnmp)
      consumer.endField("output_snmp", 16)

      consumer.startField("in_pkts", 4)
      consumer.addLong(record.inpkts)
      consumer.endField("in_pkts", 4)

      consumer.startField("in_bytes", 3)
      consumer.addLong(record.inbytes)
      consumer.endField("in_bytes", 3)

      consumer.startField("first_switched", 24)
      consumer.addLong(record.fsw)
      consumer.endField("first_switched", 24)

      consumer.startField("last_switched", 23)
      consumer.addLong(record.lsw)
      consumer.endField("last_switched", 23)

      consumer.startField("l4_src_port", 9)
      consumer.addInteger(record.srcp)
      consumer.endField("l4_src_port", 9)

      consumer.startField("l4_dst_port", 13)
      consumer.addInteger(record.dstp)
      consumer.endField("l4_dst_port", 13)

      consumer.startField("tcp_flags", 8)
      consumer.addInteger(record.flag)
      consumer.endField("tcp_flags", 8)

      consumer.startField("protocol", 6)
      consumer.addInteger(record.pro)
      consumer.endField("protocol", 6)

      consumer.startField("src_tos", 7)
      consumer.addInteger(record.srctos)
      consumer.endField("src_tos", 7)

      consumer.startField("src_as", 18)
      consumer.addLong(record.srcas)
      consumer.endField("src_as", 18)

      consumer.startField("dst_as", 19)
      consumer.addLong(record.dstas)
      consumer.endField("dst_as", 19)

      consumer.startField("src_mask", 11)
      consumer.addInteger(record.srcmask)
      consumer.endField("src_mask", 11)

      consumer.startField("dst_mask", 15)
      consumer.addInteger(record.dstmask)
      consumer.endField("dst_mask", 15)

      val bgpTuple: Array[Any] = BGPRoutingTable.pseudo
      if (bgpTuple(0) != null) {
        consumer.startField("router_prefix", 94)
        consumer.addBinary(Binary.fromByteArray(bgpTuple(0).asInstanceOf[Array[Byte]]))
        consumer.endField("router_prefix", 94)
      }

      if (bgpTuple(1) != null) {
        consumer.startField("router_ipv4", 95)
        consumer.addBinary(Binary.fromByteArray(bgpTuple(1).asInstanceOf[Array[Byte]]))
        consumer.endField("router_ipv4", 95)
      }

      if (bgpTuple(2) != null) {
        consumer.startField("router_ipv6", 96)
        consumer.addBinary(Binary.fromByteArray(bgpTuple(2).asInstanceOf[Array[Byte]]))
        consumer.endField("router_ipv6", 96)
      }

      if (bgpTuple(3) != null) {
        consumer.startField("next_hop_ipv4", 97)
        consumer.addBinary(Binary.fromByteArray(bgpTuple(3).asInstanceOf[Array[Byte]]))
        consumer.endField("next_hop_ipv4", 97)
      }

      if (bgpTuple(4) != null) {
        consumer.startField("next_hop_ipv6", 98)
        consumer.addBinary(Binary.fromByteArray(bgpTuple(4).asInstanceOf[Array[Byte]]))
        consumer.endField("next_hop_ipv6", 98)
      }

      if (bgpTuple(5) != null) {
        consumer.startField("as_path", 99)
        consumer.addBinary(Binary.fromByteArray(bgpTuple(5).asInstanceOf[Array[Byte]]))
        consumer.endField("as_path", 99)
      }

      if (bgpTuple(6) != null) {
        consumer.startField("community", 100)
        consumer.addBinary(Binary.fromByteArray(bgpTuple(6).asInstanceOf[Array[Byte]]))
        consumer.endField("community", 100)
      }

      if (bgpTuple(7) != null) {
        consumer.startField("adjacent_as", 101)
        consumer.addBinary(Binary.fromByteArray(bgpTuple(7).asInstanceOf[Array[Byte]]))
        consumer.endField("adjacent_as", 101)
      }

      if (bgpTuple(8) != null) {
        consumer.startField("self_as", 102)
        consumer.addBinary(Binary.fromByteArray(bgpTuple(8).asInstanceOf[Array[Byte]]))
        consumer.endField("self_as", 102)
      }

      consumer.endMessage()
      curRowPos += rowLength

      i += 1
    }
  }
}
