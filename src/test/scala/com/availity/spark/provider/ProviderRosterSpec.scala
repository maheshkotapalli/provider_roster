package com.availity.spark.provider

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class ProviderRosterSpec extends AnyFunSpec with DataFrameComparer with BeforeAndAfterEach {

  override def beforeEach: Unit = {
  }

  override def afterEach(): Unit = {
  }

  describe("process") {
    it("Pass Inbound and Outbound directory") {
      /*** Change the input and output path arguments to specific HDFS or s3 directory***/
      val args:Array[String]=Array("C:/projects/project_roster/data","C:/projects/project_roster/output")
      ProviderRoster.main(args)
    }
  }
}
