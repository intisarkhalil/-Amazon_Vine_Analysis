# Amazon Reviews Analysis
-	From the Amazon Review datasets  we picked the amazon review of [US-Mobile Apps](https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Mobile_Apps_v1_00.tsv.gz).
-	PySpark
-	RDS 
-	S3
## Overview of the analysis: 
Analyzing Amazon reviews written by members of the paid Amazon Vine program. The Amazon Vine program is a service that allows manufacturers and publishers to receive reviews for their products. Companies like SellBy pay a small fee to Amazon and provide products to Amazon Vine members, who are then required to publish a review.
### Purpose:
The purpose of this analysis is to use PySpark to perform the ETL process to extract the dataset, transform the data, connect to an AWS RDS instance, and load the transformed data into pgAdmin. Also use PySpark, Pandas, or SQL to determine if there is any bias toward favorable reviews from Vine members in the dataset. 
### Methods:
#### Perform ETL on Amazon Product Reviews
1. Create a new database with Amazon RDS.
2. In pgAdmin, create a new database in Amazon RDS server 
3. In pgAdmin, run a new query to create the tables for the new database.
4. After running the query, we have the following four tables database:
   - Customer table ```customers_table```, 
   - Products table ```products_table```,
   - Review table ``` review_id_table```,
   - Vine table ```vine_table```.
#### Determine Bias of Vine Reviews: The analysis does the following:
1. DataFrame or table for the ```vine_table``` data using ```PySpark```.
2. The data is filtered to create a DataFrame or table wherere there are 20 or more total votes.
3. The data is filtered to create a DataFrame or table where the percentage of ```helpful_votes``` is equal to or greater than 50% 
4. The data is filtered to create a DataFrame or table where there is a Vine review.
5. The data is filtered to create a DataFrame or table where there isn’t a Vine review.
6. The total number of reviews, the number of 5-star reviews, and the percentage 5-star reviews are calculated for all Vine and non-Vine reviews 

## Results: Using bulleted lists and images of DataFrames as support, address the following questions:

1.	How many Vine reviews and non-Vine reviews were there?
```
The number of vine reviews is 0
The number of non-vine reviews is 129508
```
2.	How many Vine reviews were 5-stars? How many non-Vine reviews were 5-stars?
```
The number of vine reviews were 5-stars is zero while, 
The non-vine reviews were 5 stars is 59271
```
3.	What percentage of Vine reviews were 5 stars? What percentage of non-Vine reviews were 5 stars?
```
The percentage of vine reviews were 5 stars is 0.00%, 
Wthe hile percentage of non-fine reviews were 5 stars is 45.766%.
```
## Summary: 
```
- Paid Reviews
0 total reviews
0 were 5-star reviews
0.0% of vine (paid) reviews were 5 star
- Unpaid Reviews
129508 total reviews
59271 were 5-star reviews
45.766 %of unpaid reviews were 5-star reviews
```
The results  show that there are 129508 reviews in the non-vine program 59271 of them were 5-star, while there isn't reviews in the Vine program, which indicates a positive bias for reviews in the vine program. Mobile Apps Reviews may have fewer reviews per product title so data combining Mobile Apps Reviews  may offer different results for number of review and participants in the Amazon Vine Program.

