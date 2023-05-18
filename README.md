<h1>DeepLearning workshop</h1>
<h2> Workshop that consisted in making a predicting algorithm using linear regression</h2>
<h3 > HOW TO <h3 />
 <h4> Prerequisites <h4 />
   <ul>
      <li> You need Kafka to simulate the data transfer from a service to another via streaming https://kafka.apache.org/quickstart </li>
      <li> I highly recommend running this on Linux as this is my development environment </li>
      <li> Python 3 </li>
      <li> Docker & Docker compose</li>
   </ul>
   
   <h4><u> Step one </u><h4/>
 Once you have started Kafka and created a topic named "topic capteur" run capteur.py
 This will create a set of data randomly generated
  
  <h4><u> Step two </u><h4/>
  run consumer.py, this will capture the data from kafka and send them to minio in json format

<h4><u>Step three </u><h4/>
  run spark_data_preparation.py, this will simulate a data cleaning process. Unfortunately I ran into some issues using pySpark I am guessing that Scala is more efficient for such exercise, used pandas instead which is more than enough given our set of datas.
  
<h4><u>Step four </u><h4 />
  run training_spark_ml.py, this will train and save an algorithm model.
  
That's it folks
