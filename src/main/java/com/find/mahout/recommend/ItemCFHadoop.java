package com.find.mahout.recommend;

import org.apache.hadoop.mapred.JobConf;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

import com.find.mahout.hadoop.HdfsDAO;

public class ItemCFHadoop {

	public static final String HDFS = "hdfs://54.255.164.52:8020";

	public static void main(String[] args) throws Exception {
		String localFile = "datafile/item.csv";
		String inPath = HDFS + "/user/hadoop/mahout";
		String inFile = inPath + "/item.csv";
		String outPath = HDFS + "/user/hadoop/mahout/result/";
		String outFile = outPath + "/part-r-00000";
		String tmpPath = HDFS + "/tmp/" + System.currentTimeMillis();

		JobConf conf = config();
		HdfsDAO hdfs = new HdfsDAO(HDFS, conf);
		hdfs.rmr(inPath);
		hdfs.mkdirs(inPath);
		hdfs.copyFile(localFile, inPath);
		hdfs.ls(inPath);
		hdfs.cat(inFile);

		StringBuilder sb = new StringBuilder();
		sb.append("--input ").append(inPath);
		sb.append(" --output ").append(outPath);
		sb.append(" --booleanData true");
		sb.append(" --similarityClassname org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.EuclideanDistanceSimilarity");
		sb.append(" --tempDir ").append(tmpPath);
		args = sb.toString().split(" ");

		RecommenderJob job = new RecommenderJob();
		job.setConf(conf);
		job.run(args);

		hdfs.cat(outFile);
	}

	public static JobConf config() {
		JobConf conf = new JobConf(ItemCFHadoop.class);
		conf.setJobName("ItemCFHadoop");
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");
		return conf;
	}
}
