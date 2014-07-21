package com.find.mahout.recommend;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.commons.cli2.OptionException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

public class UserCFMahout {

	public static void main(String[] args) throws FileNotFoundException, TasteException, IOException, OptionException {

		// 创建DataModel  slopeOne
		File ratingsFile = new File("E:\\cy-bigdata-project\\mahout-example\\src\\main\\resources\\datafile\\slopeOne.csv");
		DataModel model = new FileDataModel(ratingsFile);

		// 创建Recommender，使用SlopeOneRecommender
		CachingRecommender cachingRecommender = new CachingRecommender(new SlopeOneRecommender(model));

		// for all users
		for (LongPrimitiveIterator it = model.getUserIDs(); it.hasNext();) {
			long userId = it.nextLong();

			// get the recommendations for the user
			List<RecommendedItem> recommendations = cachingRecommender.recommend(userId, 10);

			// if empty write something
			if (recommendations.size() == 0) {
				System.out.print("User ");
				System.out.print(userId);
				System.out.println(": no recommendations");
			}

			// print the list of recommendations for each
			for (RecommendedItem recommendedItem : recommendations) {
				System.out.print("User ");
				System.out.print(userId);
				System.out.print(": ");
				System.out.println(recommendedItem);
			}
		}
	}
}
