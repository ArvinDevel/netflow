package cn.ac.ict.acs.netflow.util

import java.net.InetAddress

import com.google.common.net.InetAddresses

/**
 * Wrapper for Google Guava InetAddresses Implementation
 */
object IPUtils {

  def fromString(s: String): Array[Byte] = {
    InetAddresses.forString(s).getAddress
  }

  def toString(ab: Array[Byte]): String = {
    InetAddress.getByAddress(ab).getHostAddress
  }
}
