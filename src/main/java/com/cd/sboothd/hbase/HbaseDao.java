package com.cd.sboothd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HbaseDao {


	public static void main(String[] args) throws Exception{


		Configuration conf = HBaseConfiguration.create();

		conf.set("hbase.zookeeper.quorum", "weekend05:2181,weekend06:2181,weekend07:2181");

		//hBaseAdmin对表进行管理的客户端
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);

		TableName tableName = TableName.valueOf("mygirls");
		HTableDescriptor htd = new HTableDescriptor(tableName);

		//对列族的描述
		HColumnDescriptor base_info = new HColumnDescriptor("base_info");
		base_info.setMaxVersions(3);


		//在表描述器中添加列族
		htd.addFamily(base_info);

		hBaseAdmin.createTable(htd);

	}






}
