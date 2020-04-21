#Sairam this is a program does scrapping of amazon webpages for products and preprocess the data and will push the data onto a kafka topic in 3 node kafka systems
#WE want the following python package : python-kafka to be installed, can be done by sudo apt-get install python3-kafka.


# Take and modified accordingly from  https://www.scrapehero.com/how-to-scrape-amazon-product-reviews-using-python/  


#This is producer program so we import the required packages.
from kafka import KafkaProducer         #import kafka producer from kafka.
from kafka import KafkaProducer
from kafka.errors import KafkaError     #import kafkaError module.
import os      			        #import os module.
import json,csv   			        #import json,csv modules.
from time import sleep                  #import sleep from time to make the stream produce at particular time
from random import choice,randint,randrange
from string import digits
import datetime 
from lxml import html
from requests import get
from re import sub
from dateutil import parser as dateparser


# idea of the project is to scrape the amazon website for procducts given in the products.csv file scrape the comments and push on to hdfs using kafka connect. 

#Function for getting the review for required procducts. 
def ParseReviews(asin):
    # This script has only been tested with Amazon.com
    amazon_url  = 'http://www.amazon.com/dp/'+asin
    # Add some recent user agent to prevent amazon from blocking the request 
    # Find some chrome user agent strings  here https://udger.com/resources/ua-list/browser-detail?browser=Chrome
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'}
    for i in range(5):
        response = get(amazon_url, headers = headers, verify=False, timeout=30)
        if response.status_code == 404:
            return {"url": amazon_url, "error": "page not found"}
        if response.status_code != 200:
            continue
        # Removing the null bytes from the response.
        cleaned_response = response.text.replace('\x00', '')
        parser = html.fromstring(cleaned_response)
        XPATH_AGGREGATE = '//span[@id="acrCustomerReviewText"]'
        XPATH_REVIEW_SECTION_1 = '//div[contains(@id,"reviews-summary")]'
        XPATH_REVIEW_SECTION_2 = '//div[@data-hook="review"]'
        XPATH_AGGREGATE_RATING = '//table[@id="histogramTable"]//tr'
        XPATH_PRODUCT_NAME = '//h1//span[@id="productTitle"]//text()'
        XPATH_PRODUCT_PRICE = '//span[@id="priceblock_ourprice"]/text()'
        raw_product_price = parser.xpath(XPATH_PRODUCT_PRICE)
        raw_product_name = parser.xpath(XPATH_PRODUCT_NAME)
        total_ratings  = parser.xpath(XPATH_AGGREGATE_RATING)
        reviews = parser.xpath(XPATH_REVIEW_SECTION_1)
        product_price = ''.join(raw_product_price).replace(',', '')
        product_name = ''.join(raw_product_name).strip()
        if not reviews:
            reviews = parser.xpath(XPATH_REVIEW_SECTION_2)
        ratings_dict = {}
        reviews_list = []
                # Grabing the rating  section in product page
        for ratings in total_ratings:
            extracted_rating = ratings.xpath('./td//a//text()')
            if extracted_rating:
                rating_key = extracted_rating[0] 
                raw_raing_value = extracted_rating[1]
                rating_value = raw_raing_value
                if rating_key:
                    ratings_dict.update({rating_key: rating_value})
        # Parsing individual reviews
        for review in reviews:
            XPATH_RATING  = './/i[@data-hook="review-star-rating"]//text()'
            XPATH_REVIEW_HEADER = './/a[@data-hook="review-title"]//text()'
            XPATH_REVIEW_POSTED_DATE = './/span[@data-hook="review-date"]//text()'
            XPATH_REVIEW_TEXT_1 = './/div[@data-hook="review-collapsed"]//text()'
            XPATH_REVIEW_TEXT_2 = './/div//span[@data-action="columnbalancing-showfullreview"]/@data-columnbalancing-showfullreview'
            XPATH_REVIEW_COMMENTS = './/span[@data-hook="review-comment"]//text()'
            XPATH_AUTHOR = './/span[contains(@class,"profile-name")]//text()'
            XPATH_REVIEW_TEXT_3 = './/div[contains(@id,"dpReviews")]/div/text()'          
            raw_review_author = review.xpath(XPATH_AUTHOR)
            raw_review_rating = review.xpath(XPATH_RATING)
            raw_review_header = review.xpath(XPATH_REVIEW_HEADER)
            raw_review_posted_date = review.xpath(XPATH_REVIEW_POSTED_DATE)
            raw_review_text1 = review.xpath(XPATH_REVIEW_TEXT_1)
            raw_review_text2 = review.xpath(XPATH_REVIEW_TEXT_2)
            raw_review_text3 = review.xpath(XPATH_REVIEW_TEXT_3)
            # Cleaning data
            author = ' '.join(' '.join(raw_review_author).split())
            review_rating = ''.join(raw_review_rating).replace('out of 5 stars', '')
            review_header = ' '.join(' '.join(raw_review_header).split())
            try:
                review_posted_date = dateparser.parse(''.join(raw_review_posted_date)).strftime('%d %b %Y')
            except:
                review_posted_date = None
            review_text = ' '.join(' '.join(raw_review_text1).split())
            # Grabbing hidden comments if present
            if raw_review_text2:
                json_loaded_review_data = loads(raw_review_text2[0])
                json_loaded_review_data_text = json_loaded_review_data['rest']
                cleaned_json_loaded_review_data_text = re.sub('<.*?>', '', json_loaded_review_data_text)
                full_review_text = review_text+cleaned_json_loaded_review_data_text
            else:
                full_review_text = review_text
            if not raw_review_text1:
                full_review_text = ' '.join(' '.join(raw_review_text3).split())
            raw_review_comments = review.xpath(XPATH_REVIEW_COMMENTS)
            review_comments = ''.join(raw_review_comments)
            review_comments = sub('[A-Za-z]', '', review_comments).strip()
            review_dict = {
                                'review_comment_count': review_comments,
                                'review_text': full_review_text,
                                'review_posted_date': review_posted_date,
                                'review_header': review_header,
                                'review_rating': review_rating,
                                'review_author': author
                            }
            reviews_list.append(review_dict)
        data = {
                    'ratings': ratings_dict,
                    'reviews': reviews_list,
                    'url': amazon_url,
                    'name': product_name,
                    'price': product_price
                
                }
        return data
    return {"error": "failed to process the page", "url": amazon_url}




def ReadAsin():
    # Add your own ASINs here
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    AsinList = csv.DictReader(open(('./products.csv')))
#    AsinList = ['B01ETPUQ6E', 'B017HW9DEW', 'B00U8KSIOM']
    extracted_data = []
    
    for asin in AsinList:
        print("Downloading and processing page http://www.amazon.com/dp/" + asin['productId'])
        extracted_data.append(ParseReviews(asin['productId']))
        producer.send('Amazon_product_reviews.',json.dumps(extracted_data).encode('utf-8'))
        sleep(5)

        producer.flush()
        producer = KafkaProducer(retries=5)


#    f = open('data.json', 'w')
#    dump(extracted_data, f, indent=4)
#    f.close()

#
#while True:
#    transaction = new_transaction()
#    print(transaction)
#    count=count+1
#    print(count)
#    producer.send('Amazon_product_reviews.',json.dumps(transaction).encode('utf-8'))
#    sleep(2)	
    
    


if __name__ == '__main__':
    
    ReadAsin()

