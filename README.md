**

# No-SQL lab1 Mapreduce programming submission

## Link to Problems :

 [https://moodle.daiict.ac.in/pluginfile.php/8971/mod_assign/introattachment/0/Lab01.pdf?forcedownload=1](https://moodle.daiict.ac.in/pluginfile.php/8971/mod_assign/introattachment/0/Lab01.pdf?forcedownload=1)

## Solutions:

Q-1) Installation of hadoop done on Ubuntu - 19.10.   

Q-2) Running MaxTemperature example on eclipse. 
Reference: https://youtu.be/WoZ2KSAfujQ (in this video, wordCount example is explained and guided how to run it on eclipse)

Q-3) Output of mapper class is (image_type as text, 1 as IntWritable)
		 Output of reducer class is (image_type as text, totalCount as IntWritable)

Q-4) Example line of web access log as input line: 
	109.169.248.247 - - **[12/Dec/2015:18:25:11** +0100] "GET /administrator/ HTTP/1.1" 200 **4263** "-" "Mozilla/5.0 (Windows NT 6.0; rv:34.0) Gecko/20100101 Firefox/34.0" "-"
	So, [12/**Dec/2015**:18:25:11 is date and time token. From which we extract key as 
 Dec 2015. Value will be bytes transferred. We extract value 4263 in this case.
 So, output of mapper class is (month-year as text, totalBytesTransferred as IntWritable)
Output of reducer class is (month-year-toalMegabytesTranferred as Text, RequestCounts as IntWritable)

Q-5) Example line of web access log as input line: 
191.182.199.16 - - [12/Dec/2015:19:02:36 +0100] "GET /templates/_system/css/general.css HTTP/1.1" **404** 239 **"http://almhuette-raith.at/"** "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36" "-"
We extract http response status as 404 here and similarly url.
So, output of mapper class is (404 as String, url as String)
This is map only mapreduce job. So, no reducer class.
And in driver class, we set:

**setNumReduceTasks(0);**
 


