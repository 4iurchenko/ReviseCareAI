#!/usr/bin/env python
# coding: utf-8

# ## ReviseCareAI_ReviewsGrabber
# 
# New notebook

# In[1]:


# Copy the output to the processing notebook as a first cell
# Remove all unnecesary hospitals
# ******
hospitals_config = '''
Scripps Mercy Hospital San Diego:ChIJG1uXAtpU2YARlrrtB9mAqjw
Sharp Memorial Hospital:ChIJ8Q0HQF9V2YAR9clz3L9QTHw
Kindred Hospital San Diego:ChIJb-g3WeFU2YARsXx7bJpLPpA
Hillcrest Medical Center at UC San Diego Health:ChIJ19C1PtZU2YARoB1ec6RBNoA
Naval Medical Center:ChIJK1gnL6dU2YARVmtye1FMG7w
Select Specialty Hospital - San Diego:ChIJDUfwvx1V2YARkzwov5fQ1es
Kaiser Permanente San Diego Medical Center:ChIJASS3IEv-24ARWc3gkB-I7cA
Rady Children’s Hospital - San Diego:ChIJSZqBu9dU2YARhhpz0EGLooo
Scripps Memorial Hospital La Jolla:ChIJK1n7it4G3IARtIq4C6lYJ2I
Vibra Hospital of San Diego:ChIJjYO8adpU2YAR4I_h-DE6NH0
Sharp Mary Birch Hospital for Women & Newborns:ChIJ99-RUl9T2YARIYBo3w54zF8
Scripps Mercy Hospital Chula Vista:ChIJg-Mx4uNN2YAR5Vl8xGWVhPU
Paradise Valley Hospital:ChIJQy5TnylS2YARqzvb9ZDWsVg
Sharp Grossmont Hospital:ChIJ-718aG1X2YARWrmlhhk4aJA
Kaiser Permanente Zion Medical Center:ChIJr80_QsJV2YARkUg6KqwSUuo
UCSD HILLCREST:ChIJk3jlG8VV2YARtD8bzFNhHzY
UC San Diego Division of Hospital Medicine:ChIJm1IklZlw44kRENEZvJgIuBc
Sharp Coronado Hospital:ChIJDQfFKEpT2YARAWp6gvF8NyU
VA Medical Center-San Diego:ChIJy6yiHdAG3IAR97uNw1l9xE4
Jacobs Medical Center at UC San Diego Health:ChIJb9IXHdoG3IARYVL4U5OQ7y8
'''


# In[2]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %run ReviseCareAI_Utils


# In[3]:


place_ids = get_ids_from_config(hospitals_config)
print(place_ids)


# In[4]:


all_reviews_df = None

for i in place_ids[0:21]:
    details = get_hospital_details(place_id = i)
    append_df = list_to_dataframe(data = [details])
    
    if all_reviews_df is None:
        all_reviews_df = append_df
    else:
        all_reviews_df = all_reviews_df.unionAll(append_df)
    

display(all_reviews_df)


# In[5]:


from pyspark.sql import Window
from pyspark.sql.functions import explode, col, current_date, from_unixtime, date_sub, row_number
import pyspark.sql.functions as F

all_reviews_exploded_df = (
    all_reviews_df
    .withColumn("processing_date", current_date())
    .withColumn("review_details", explode(col("reviews")))
    .drop("reviews")
    .select(
            col('processing_date').alias('processing_date'),
            col('business_status').alias('hospital_status'),
            col('formatted_address').alias('hospital_address'),
            col('name').alias('hospital_name'),
            col('place_id').alias('hospital_place_id'),
            col('rating').alias('hospital_avg_rating'),
            col('user_ratings_total').alias('hospital_rating_cnt'),
            col('review_details.author_name').alias('review_author'),
            col('review_details.author_url').alias('review_author_url'),
            col('review_details.rating').alias('review_rating'),
            col('review_details.text').alias('review_text'),
            from_unixtime(col('review_details.time')).alias('review_time'),
    )    
)

display(all_reviews_exploded_df)


# # Writing to enriched_daily_reviews

# In[6]:


processing_date = str(all_reviews_exploded_df.agg(F.max("processing_date")).collect()[0][0])

(
all_reviews_exploded_df.write
.format("delta")
.mode("overwrite")
.option("replaceWhere", f"processing_date = '{processing_date}'")
.save("Files/enriched_daily_reviews")
)


# In[7]:


daily_reviews_df = spark.read.format("delta").load("Files/enriched_daily_reviews")
display(daily_reviews_df)


# # Deduplication to "enriched_final_reviews"

# In[8]:


window_spec = Window.partitionBy("hospital_place_id", "review_author_url").orderBy(col("processing_date").desc())
df_with_row_number = daily_reviews_df.withColumn("row_num", row_number().over(window_spec))
dedup_reviews_df = df_with_row_number.filter(col("row_num") == 1).drop("row_num")


# In[9]:


(
dedup_reviews_df.write
.format("delta")
.mode("overwrite")
.save("Files/enriched_final_reviews")
)

enriched_final_reviews_df = spark.read.format("delta").load("Files/enriched_final_reviews")
display(enriched_final_reviews_df)

