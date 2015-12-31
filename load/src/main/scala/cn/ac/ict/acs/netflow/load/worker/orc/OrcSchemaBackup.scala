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
/**
  * Created by arvin on 15-12-26.
  *
  *
  *
  */
package cn.ac.ict.acs.netflow.load.worker.orc

import cn.ac.ict.acs.netflow.Logging

// currently can't make sure that JavaConstantbinaryObjectInspector
// is which type's reference ,maybe not "binary"
// 12.26 Arvin
object OrcSchemaBackup extends Logging{

  val orcSchemaWritable = new Array[Any](115)
  val columns = new Array[String](115)
  val columnsTypes = new Array[String](115)

  private val headerFields = new Array[String](5)
  private val headerFieldsTypes = new Array[String](5)
  headerFields(0) = "time"
  headerFields(1) = "router_ipv4"
  headerFields(2) =  "router_ipv6"
  headerFields(3) = null
  headerFields(4) = null
  // ToDo: make sure types name in ORc
  headerFieldsTypes(0) = "bigint"
  headerFieldsTypes(1) = "binary"
  headerFieldsTypes(2) = "binary"
  headerFieldsTypes(3) = null
  headerFieldsTypes(4) = null


  private val bgpFields = new Array[String](9)
  private val bgpFieldsTypes = new Array[String](9)
  bgpFields(0) = "router_prefix"
  bgpFields(1) =  "router_ipv4"
  bgpFields(2) = "router_ipv6"
  bgpFields(3) = "next_hop_ipv4"
  bgpFields(4) =  "next_hop_ipv6"
  bgpFields(5) =  "as_path"
  bgpFields(6) =  "community"
  bgpFields(7) =  "adjacent_as"
  bgpFields(8) =  "self_as"
  bgpFieldsTypes(0) = "binary"
  bgpFieldsTypes(1) = "binary"
  bgpFieldsTypes(2) = "binary"
  bgpFieldsTypes(3) = "binary"
  bgpFieldsTypes(4) = "binary"
  bgpFieldsTypes(5) = "binary"
  bgpFieldsTypes(6) = "binary"
  bgpFieldsTypes(7) = "binary"
  bgpFieldsTypes(8) = "binary"

  private val netflowFields = new Array[String](101)
  private val netflowFieldsTypes = new Array[String](101)
  netflowFields(0) = null
  // 1-10
  netflowFields(1) =  "in_bytes"
  netflowFields(2) =  "in_pkts"
  netflowFields(3) =  "flows"
  netflowFields(4) =  "protocol"
  netflowFields(5) =  "src_tos"
  netflowFields(6) =  "tcp_flags"
  netflowFields(7) =  "l4_src_port"
  netflowFields(8) =  "ipv4_src_addr"
  netflowFields(9) =  "src_mask"
  netflowFields(10) =  "input_snmp"

  // 11-20
  netflowFields(11) =  "l4_dst_port"
  netflowFields(12) =  "ipv4_dst_addr"
  netflowFields(13) =  "dst_mask"
  netflowFields(14) =  "output_snmp"
  netflowFields(15) =  "ipv4_next_hop"
  netflowFields(16) =  "src_as"
  netflowFields(17) =  "dst_as"
  netflowFields(18) =  "bgp_ipv4_next_hop"
  netflowFields(19) =  "mul_dst_pkts"
  netflowFields(20) =  "mul_dst_bytes"

  // 21-30
  netflowFields(21) =  "last_switched"
  netflowFields(22) =  "first_switched"
  netflowFields(23) =  "out_bytes"
  netflowFields(24) =  "out_pkts"
  netflowFields(25) =  "min_pkt_lngth"
  netflowFields(26) =  "max_pkt_lngth"
  netflowFields(27) =  "ipv6_src_addr"
  netflowFields(28) =  "ipv6_dst_addr"
  netflowFields(29) =  "ipv6_src_mask"
  netflowFields(30) =  "ipv6_dst_mask"

  // 31-40
  netflowFields(31) =  "ipv6_flow_label" // ipv6 only use 20bit
  netflowFields(32) =  "icmp_type"
  netflowFields(33) =  "mul_igmp_type"
  netflowFields(34) =  "sampling_interval"
  netflowFields(35) =  "sampling_algorithm"
  netflowFields(36) =  "flow_active_timeout"
  netflowFields(37) =  "flow_inactive_timeout"
  netflowFields(38) =  "engine_type"
  netflowFields(39) =  "engine_id"
  netflowFields(40) =  "total_bytes_exp"

  // 41-50
  netflowFields(41) =  "total_pkts_exp"
  netflowFields(42) =  "total_flows_exp"
  netflowFields(43) = null // *Vendor Proprietary
  netflowFields(44) =  "ipv4_src_prefix"
  netflowFields(45) =  "ipv4_dst_prefix"
  netflowFields(46) =  "mpls_top_label_type"
  netflowFields(47) =  "mpls_top_label_ip_addr"
  netflowFields(48) =  "flow_sampler_id"
  netflowFields(49) =  "flow_sampler_mode"
  netflowFields(50) =  "flow_sampler_random_interval"

  // 51-60
  netflowFields(51) = null // *Vendor Proprietary
  netflowFields(52) =  "min_ttl"
  netflowFields(53) =  "max_ttl"
  netflowFields(54) =  "ipv4_ident"
  netflowFields(55) =  "dst_tos"
  netflowFields(56) =  "in_src_mac"
  netflowFields(57) =  "out_dst_mac"
  netflowFields(58) =  "src_vlan"
  netflowFields(59) =  "dst_vlan"
  netflowFields(60) =  "ip_protocol_version"

  // 61-70
  netflowFields(61) =  "direction"
  netflowFields(62) =  "ipv6_next_hop"
  netflowFields(63) =  "bpg_ipv6_next_hop"
  netflowFields(64) =  "ipv6_option_headers"
  netflowFields(65) = null // *Vendor Proprietary
  netflowFields(66) = null // *Vendor Proprietary
  netflowFields(67) = null // *Vendor Proprietary
  netflowFields(68) = null // *Vendor Proprietary
  netflowFields(69) = null // *Vendor Proprietary
  netflowFields(70) =  "mpls_label_1" // contain 20bit

  // 71-80
  netflowFields(71) =  "mpls_label_2" // contain 20bit
  netflowFields(72) =  "mpls_label_3" // contain 20bit
  netflowFields(73) =  "mpls_label_4" // contain 20bit
  netflowFields(74) =  "mpls_label_5" // contain 20bit
  netflowFields(75) =  "mpls_label_6" // contain 20bit
  netflowFields(76) =  "mpls_label_7" // contain 20bit
  netflowFields(77) =  "mpls_label_8" // contain 20bit
  netflowFields(78) =  "mpls_label_9" // contain 20bit
  netflowFields(79) =  "mpls_label_10" // contain 20bit
  netflowFields(80) =  "in_dst_mac"

  // 81-90
  netflowFields(81) =  "out_src_mac"
  netflowFields(82) =  "if_name"
  netflowFields(83) =  "if_desc"
  netflowFields(84) =  "sampler_name"
  netflowFields(85) =  "in_permanent_bytes"
  netflowFields(86) =  "in_permanent_pkts"
  netflowFields(87) = null // *Vendor Proprietary
  netflowFields(88) =  "fragment_offset"
  netflowFields(89) =  "forwarding_status"
  netflowFields(90) =  "mpls_pal_rd"

  // 91-100
  netflowFields(91) =  "mpls_prefix_len"
  netflowFields(92) =  "src_traffic_index"
  netflowFields(93) =  "dst_traffic_index"
  netflowFields(94) =  "application_description"
  netflowFields(95) =  "application_tag"
  netflowFields(96) =  "application_name"
  netflowFields(97) = null // *Vendor Proprietary
  netflowFields(98) =  "postipdiffservcodepoint"
  netflowFields(99) =  "rireplication_factor"
  netflowFields(100) =  "DEPRECATED"

  netflowFieldsTypes(0) = null
  // 1-10
  netflowFieldsTypes(1) = "bigint"
  netflowFieldsTypes(2) = "bigint"
  netflowFieldsTypes(3) = "bigint"
  netflowFieldsTypes(4) = "int"
  netflowFieldsTypes(5) = "int"
  netflowFieldsTypes(6) = "int"
  netflowFieldsTypes(7) = "int"
  netflowFieldsTypes(8) = "binary"
  netflowFieldsTypes(9) = "int"
  netflowFieldsTypes(10) = "bigint"

  // 11-20
  netflowFieldsTypes(11) = "int"
  netflowFieldsTypes(12) = "binary"
  netflowFieldsTypes(13) = "int"
  netflowFieldsTypes(14) = "bigint"
  netflowFieldsTypes(15) = "binary"
  netflowFieldsTypes(16) = "bigint"
  netflowFieldsTypes(17) = "bigint"
  netflowFieldsTypes(18) = "binary"
  netflowFieldsTypes(19) = "bigint"
  netflowFieldsTypes(20) = "bigint"

  // 21-30
  netflowFieldsTypes(21) = "bigint"
  netflowFieldsTypes(22) = "bigint"
  netflowFieldsTypes(23) = "bigint"
  netflowFieldsTypes(24) = "bigint"
  netflowFieldsTypes(25) = "int"
  netflowFieldsTypes(26) = "int"
  netflowFieldsTypes(27) = "binary"
  netflowFieldsTypes(28) = "binary"
  netflowFieldsTypes(29) = "int"
  netflowFieldsTypes(30) = "int"

  // 31-40
  netflowFieldsTypes(31) = "int" // ipv6 only use 20bit
  netflowFieldsTypes(32) = "int"
  netflowFieldsTypes(33) = "int"
  netflowFieldsTypes(34) = "bigint"
  netflowFieldsTypes(35) = "int"
  netflowFieldsTypes(36) = "int"
  netflowFieldsTypes(37) = "int"
  netflowFieldsTypes(38) = "int"
  netflowFieldsTypes(39) = "int"
  netflowFieldsTypes(40) = "bigint"

  // 41-50
  netflowFieldsTypes(41) = "bigint"
  netflowFieldsTypes(42) = "bigint"
  netflowFieldsTypes(43) = null // *Vendor Proprietary
  netflowFieldsTypes(44) = "bigint"
  netflowFieldsTypes(45) = "bigint"
  netflowFieldsTypes(46) = "int"
  netflowFieldsTypes(47) = "binary"
  netflowFieldsTypes(48) = "int"
  netflowFieldsTypes(49) = "int"
  netflowFieldsTypes(50) = "bigint"

  // 51-60
  netflowFieldsTypes(51) = null // *Vendor Proprietary
  netflowFieldsTypes(52) = "int"
  netflowFieldsTypes(53) = "int"
  netflowFieldsTypes(54) = "int"
  netflowFieldsTypes(55) = "int"
  netflowFieldsTypes(56) = "binary"
  netflowFieldsTypes(57) = "binary"
  netflowFieldsTypes(58) = "int"
  netflowFieldsTypes(59) = "int"
  netflowFieldsTypes(60) = "int"

  // 61-70
  netflowFieldsTypes(61) = "int"
  netflowFieldsTypes(62) = "binary"
  netflowFieldsTypes(63) = "binary"
  netflowFieldsTypes(64) = "bigint"
  netflowFieldsTypes(65) = null // *Vendor Proprietary
  netflowFieldsTypes(66) = null // *Vendor Proprietary
  netflowFieldsTypes(67) = null // *Vendor Proprietary
  netflowFieldsTypes(68) = null // *Vendor Proprietary
  netflowFieldsTypes(69) = null // *Vendor Proprietary
  netflowFieldsTypes(70) = "int" // contain 20bit

  // 71-80
  netflowFieldsTypes(71) = "int" // contain 20bit
  netflowFieldsTypes(72) = "int"  // contain 20bit
  netflowFieldsTypes(73) = "int" // contain 20bit
  netflowFieldsTypes(74) = "int" // contain 20bit
  netflowFieldsTypes(75) = "int"  // contain 20bit
  netflowFieldsTypes(76) = "int"  // contain 20bit
  netflowFieldsTypes(77) = "int"  // contain 20bit
  netflowFieldsTypes(78) = "int"  // contain 20bit
  netflowFieldsTypes(79) = "int"  // contain 20bit
  netflowFieldsTypes(80) = "binary"

  // 81-90
  netflowFieldsTypes(81) = "binary"
  netflowFieldsTypes(82) = "binary"
  netflowFieldsTypes(83) = "binary"
  netflowFieldsTypes(84) = "binary"
  netflowFieldsTypes(85) = "bigint"
  netflowFieldsTypes(86) = "bigint"
  netflowFieldsTypes(87) = null // *Vendor Proprietary
  netflowFieldsTypes(88) = "int"
  netflowFieldsTypes(89) = "binary"
  netflowFieldsTypes(90) = "binary"

  // 91-100
  netflowFieldsTypes(91) = "int"
  netflowFieldsTypes(92) = "bigint"
  netflowFieldsTypes(93) = "bigint"
  netflowFieldsTypes(94) = "binary"
  netflowFieldsTypes(95) = "binary"
  netflowFieldsTypes(96) = "binary"
  netflowFieldsTypes(97) = null // *Vendor Proprietary
  netflowFieldsTypes(98) = "int"
  netflowFieldsTypes(99) = "bigint"
  netflowFieldsTypes(100) = "binary"

  val overallSchema = headerFields.filter(_ != null) ++
    bgpFields.filter(_ != null) ++ netflowFields.filter(_ != null)

  val overallSchemaTypes = headerFieldsTypes.filter(_ != null) ++
  bgpFieldsTypes.filter(_ != null) ++
    netflowFieldsTypes.filter(_ != null)


}

