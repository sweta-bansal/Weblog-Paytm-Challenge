
# ASSUMPTIONS


1. The inactivity threshold has been set to 900 secs (15 mins)
2. Unique session is identified by client IP, startTime, endTime and inactivity threshold


# OUTPUT



 ------------- PART 1 : SESSIONIZE WITH ASSUMED 15 MINUTE WINDOW -----------------


+---------+---------------+-------------------+-------------+
|SessionID|client         |StartTime          |SessionLength|
+---------+---------------+-------------------+-------------+
|0        |27.97.124.172  |2015-07-22 02:40:06|900          |
|1        |1.39.14.113    |2015-07-22 02:40:06|900          |
|2        |117.197.179.139|2015-07-22 02:40:06|900          |
|3        |115.250.16.146 |2015-07-22 02:40:06|900          |
|4        |112.79.36.98   |2015-07-22 02:40:06|31139        |
|5        |52.74.219.71   |2015-07-22 02:40:06|9901         |
|6        |119.81.61.166  |2015-07-22 02:40:06|9902         |
|7        |106.51.235.133 |2015-07-22 02:40:06|900          |
|8        |15.211.153.78  |2015-07-22 02:40:06|16306        |
|9        |74.125.63.33   |2015-07-22 02:40:06|10123        |
|10       |49.156.68.161  |2015-07-22 02:40:06|900          |


------------- PART 2 : FIND AVERAGE SESSION DURATION -----------------


+------------------+
|  Avg_Session_Time|
+------------------+
|3004.5253124013175|
+------------------+


------------- PART 3 : FIND UNIQUE URL HITS PER SESSION -----------------

+------------+--------------------------------------------------------------------------------------------------------------------------------------+
|SessionID   |URL                                                                                                                                   |
+------------+--------------------------------------------------------------------------------------------------------------------------------------+
|249108103857|https://paytm.com:443/offer/wp-content/plugins/images-thumbnail-sliderv1/css/images-thumbnail-sliderv1-style.css?ver=4.2.2            |
|249108103871|https://paytm.com:443/offer/wp-content/plugins/owl-carousel/css/owl.transitions.css?ver=4.2.2                                         |
|249108103871|https://paytm.com:443/offer/wp-content/plugins/wp-thumbnail-slider/js/wpt-js.js?ver=4.2.2                                             |
|257698037820|https://paytm.com:443/shop/cart                                                                                                       |
|249108103857|https://paytm.com:443/scripts/web/config/UrlConfig.js                                                                                 |
|257698038045|https://paytm.com:443/papi/rr/products/17510238/statistics?channel=web&version=2                                                      |
|257698037997|https://paytm.com:443/papi/rr/products/5820726/statistics?channel=web&version=2                                                       |
|266287972403|https://paytm.com:443/shop/orderhistory?pagesize=10&channel=web&version=2                                                             |
|266287972682|https://paytm.com:443/offer?utm_source=Affiliates&utm_medium=OMG&utm_campaign=OMG&utm_term=762154_                                    |
|266287972714|https://paytm.com:443/shop/h/electronics/kitchen-appliances                                                                           |
|249108103744|https://paytm.com:443/security                                                                                                        |





------------- PART 4 : FIND MOST ENGAGED USERS -----------------

+--------------+------------------------+--------------------------+
|client        |Total_SessionLength_Secs|Average_SessionLength_Secs|
+--------------+------------------------+--------------------------+
|220.226.206.7 |78001                   |6000.076923076923         |
|54.183.255.140|75455                   |7545.5                    |
|107.23.255.12 |75435                   |7543.5                    |
|54.252.79.172 |75432                   |7543.2                    |
|185.20.4.220  |75372                   |7537.2                    |
|54.250.253.236|75335                   |7533.5                    |
|177.71.207.172|75334                   |7533.4                    |
|120.29.232.107|75331                   |7533.1                    |
|52.74.219.71  |75322                   |7532.2                    |
|54.251.151.39 |75319                   |7531.9                    |
|54.252.254.204|75315                   |7531.5                    |
|106.186.23.95 |75314                   |7531.4                    |



