/**
  * Created by arvin on 15-12-26.
  */
package cn.ac.ict.acs.netflow.load.worker.orc

import cn.ac.ict.acs.netflow.load.worker.DataFlowSet

// To convert NetFlow schema Type to ORC  Type, and vice versa.

private[load] object Converter{
  // def toNetFlowType(metastoreType: String)



  // NetFlow dosen't define data Type, NetFlowSet just has DataFlowSet and Row conception.
  // maybe define schema just like parquet is easy.
  // can custom NetFlow DataType improve performance, just like Spark using self internal Type?
//  def toMetastoreType(dt: DataFlowSet): String = dt match {

//    case ArrayType(elementType, _) => s"array<${toMetastoreType(elementType)}>"
//    case StructType(fields) =>
//      s"struct<${fields.map(f => s"${f.name}:${toMetastoreType(f.dataType)}").mkString(",")}>"
//    case MapType(keyType, valueType, _) =>
//      s"map<${toMetastoreType(keyType)},${toMetastoreType(valueType)}>"
//    case StringType => "string"
//    case FloatType => "float"
//    case IntegerType => "int"
//    case ByteType => "tinyint"
//    case ShortType => "smallint"
//    case DoubleType => "double"
//    case LongType => "bigint"
//    case BinaryType => "binary"
//    case BooleanType => "boolean"
//    case DateType => "date"
//    case d: DecimalType => decimalMetastoreString(d)
//    case TimestampType => "timestamp"
//    case NullType => "void"
//    case udt: UserDefinedType[_] => toMetastoreType(udt.sqlType)
//  }
}

