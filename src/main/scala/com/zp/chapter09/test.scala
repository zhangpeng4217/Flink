package com.zp.chapter09

object test {
  def main(args: Array[String]): Unit = {
    val slice_t1 =
      s"""
         |select id
         |       ,sql_txt
         |    from bigdata_center.slice_t1_config
         |    where status = 1
         |    and status = 2
         |    and status = 3
         |    and status = 5
         |""".stripMargin
    println(slice_t1)
  }
}
