# Data science with Python and Netezza Performance Server

After the release of _Netezza Performance Server on Cloud Pak for Data_ (Netezza on Cloud) and _Netezza Performance Server on Cloud Pak for Data System_ (Netezza on prem) the next step in the modernization of Netezza is the ability to couple Netezza's in database analytics with Python's data science and visualization capabilities. 

With the release of [nzpy](https://pypi.org/project/nzpy/) you can connect to and work with Netezza on Cloud and Netezza on prem from any OS that support python. It even unlocks the ability to stream data directly to and from the Netezza system without any other software or drivers.

Lets look at two parts 

- First part will cover programming basics with nzpy
- The second will put everything together and actually go through a simple data science use case


## Nzpy programming basics

`nzpy` is a pure python driver implementation of the [Python DBAPI 2.0](https://www.python.org/dev/peps/pep-0249/DBApi) and so apart from a `pip install` there is no other pre-requisite.


### Connecting to the database 

The connection only requires the hostname of the Netezza 


```python
import nzpy
import os

# assume NZ_USER, NZ_PASSWORD, NZ_DATABASE and NZ_HOST are set
con = nzpy.connect(user=os.environ["NZ_USER"], password=os.environ["NZ_PASSWORD"], host=os.environ["NZ_HOST"],
                   database=os.environ["NZ_DATABASE"], port=5480)
```

The `con` is a Python DBAPI [Connection](https://www.python.org/dev/peps/pep-0249/#connection-objects) that lets one interact with the Netezza database using its [Cursor](https://www.python.org/dev/peps/pep-0249/#cursor-objects) 

### Working with Netezza database 

The `con` and `con.cursor()` objects can be used to interact with the Netezza database. [pandas.Dataframe](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) can be used for quick result visualization in addition to its main purpose as the foundation of data science.  For example


```python
import pandas as pd
result = pd.read_sql_query("select database, owner, createdate from _v_database where database like ^?^", con,
                              params=('tpc%',))
# make result column names human friendly
result.columns = [c.decode().lower() for c in result.columns]
result
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>database</th>
      <th>owner</th>
      <th>createdate</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>TPC</td>
      <td>ADMIN</td>
      <td>2020-10-12 13:34:55</td>
    </tr>
    <tr>
      <th>1</th>
      <td>TPCH</td>
      <td>ADMIN</td>
      <td>2020-10-13 19:09:48</td>
    </tr>
    <tr>
      <th>2</th>
      <td>TPCQ</td>
      <td>ADMIN</td>
      <td>2020-10-12 18:34:12</td>
    </tr>
  </tbody>
</table>
</div>



`nzpy` also supports streaming data loads and unloads. There are quite a few variations we can do here. 

#### Load or unload regular flat files 

Unloading data to local files use streaming transient external table wiht `REMOTESOURCE 'python'` option like this.


```python
with con.cursor() as cursor:
    cursor.execute('''
      create external table '/tmp/orders.csv'
        using (
            delim '|' 
            remotesource 'python'
            includeheader yes
        ) as select * from tpch..orders limit 20''')    
pd.read_csv('/tmp/orders.csv', delimiter='|')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>O_ORDERKEY</th>
      <th>O_CUSTKEY</th>
      <th>O_ORDERSTATUS</th>
      <th>O_TOTALPRICE</th>
      <th>O_ORDERDATE</th>
      <th>O_ORDERPRIORITY</th>
      <th>O_CLERK</th>
      <th>O_SHIPPRIORITY</th>
      <th>O_COMMENT</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>81696</td>
      <td>42518</td>
      <td>F</td>
      <td>234702.52</td>
      <td>1994-04-05</td>
      <td>2-HIGH</td>
      <td>Clerk#000000065</td>
      <td>0</td>
      <td>furiously regular packages are according to th...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>81700</td>
      <td>46552</td>
      <td>F</td>
      <td>68160.48</td>
      <td>1992-05-13</td>
      <td>5-LOW</td>
      <td>Clerk#000000215</td>
      <td>0</td>
      <td>quickly bold dependencies after the special pa...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>81728</td>
      <td>4279</td>
      <td>O</td>
      <td>58585.08</td>
      <td>1995-03-08</td>
      <td>4-NOT SPECIFIED</td>
      <td>Clerk#000000233</td>
      <td>0</td>
      <td>ironic instructions sleep slyly. pending, fina...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>81732</td>
      <td>46277</td>
      <td>F</td>
      <td>130629.61</td>
      <td>1992-07-13</td>
      <td>1-URGENT</td>
      <td>Clerk#000000230</td>
      <td>0</td>
      <td>bold excuses wake among the regularly final pack</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3</td>
      <td>123314</td>
      <td>F</td>
      <td>193846.25</td>
      <td>1993-10-14</td>
      <td>5-LOW</td>
      <td>Clerk#000000955</td>
      <td>0</td>
      <td>deposits alongside of the dependencies are slo...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>7</td>
      <td>39136</td>
      <td>O</td>
      <td>252004.18</td>
      <td>1996-01-10</td>
      <td>2-HIGH</td>
      <td>Clerk#000000470</td>
      <td>0</td>
      <td>ironic, regular deposits are. ironic foxes sl</td>
    </tr>
    <tr>
      <th>6</th>
      <td>35</td>
      <td>127588</td>
      <td>O</td>
      <td>253724.56</td>
      <td>1995-10-23</td>
      <td>4-NOT SPECIFIED</td>
      <td>Clerk#000000259</td>
      <td>0</td>
      <td>fluffily regular pinto beans</td>
    </tr>
    <tr>
      <th>7</th>
      <td>39</td>
      <td>81763</td>
      <td>O</td>
      <td>341734.47</td>
      <td>1996-09-20</td>
      <td>3-MEDIUM</td>
      <td>Clerk#000000659</td>
      <td>0</td>
      <td>furiously unusual pinto beans above the furiou...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>67</td>
      <td>56614</td>
      <td>O</td>
      <td>169405.01</td>
      <td>1996-12-19</td>
      <td>4-NOT SPECIFIED</td>
      <td>Clerk#000000547</td>
      <td>0</td>
      <td>regular, bold foxes across the even requests d...</td>
    </tr>
    <tr>
      <th>9</th>
      <td>71</td>
      <td>3373</td>
      <td>O</td>
      <td>276992.74</td>
      <td>1998-01-24</td>
      <td>4-NOT SPECIFIED</td>
      <td>Clerk#000000271</td>
      <td>0</td>
      <td>furiously ironic dolphins sleep slyly. careful...</td>
    </tr>
    <tr>
      <th>10</th>
      <td>99</td>
      <td>88910</td>
      <td>F</td>
      <td>112126.95</td>
      <td>1994-03-13</td>
      <td>4-NOT SPECIFIED</td>
      <td>Clerk#000000973</td>
      <td>0</td>
      <td>carefully regular theodolites may believe unu</td>
    </tr>
    <tr>
      <th>11</th>
      <td>103</td>
      <td>29101</td>
      <td>O</td>
      <td>126990.79</td>
      <td>1996-06-20</td>
      <td>4-NOT SPECIFIED</td>
      <td>Clerk#000000090</td>
      <td>0</td>
      <td>carefully ironic deposits are quickly blithely...</td>
    </tr>
    <tr>
      <th>12</th>
      <td>131</td>
      <td>92749</td>
      <td>F</td>
      <td>130464.09</td>
      <td>1994-06-08</td>
      <td>3-MEDIUM</td>
      <td>Clerk#000000625</td>
      <td>0</td>
      <td>special courts wake blithely accordin</td>
    </tr>
    <tr>
      <th>13</th>
      <td>135</td>
      <td>60481</td>
      <td>O</td>
      <td>213713.99</td>
      <td>1995-10-21</td>
      <td>4-NOT SPECIFIED</td>
      <td>Clerk#000000804</td>
      <td>0</td>
      <td>accounts cajole. final, pending dependencies a</td>
    </tr>
    <tr>
      <th>14</th>
      <td>163</td>
      <td>87760</td>
      <td>O</td>
      <td>202379.28</td>
      <td>1997-09-05</td>
      <td>3-MEDIUM</td>
      <td>Clerk#000000379</td>
      <td>0</td>
      <td>carefully express pinto beans serve carefully ...</td>
    </tr>
    <tr>
      <th>15</th>
      <td>167</td>
      <td>119402</td>
      <td>F</td>
      <td>71124.35</td>
      <td>1993-01-04</td>
      <td>4-NOT SPECIFIED</td>
      <td>Clerk#000000731</td>
      <td>0</td>
      <td>express warhorses wake carefully furiously iro...</td>
    </tr>
    <tr>
      <th>16</th>
      <td>195</td>
      <td>135421</td>
      <td>F</td>
      <td>191542.31</td>
      <td>1993-12-28</td>
      <td>3-MEDIUM</td>
      <td>Clerk#000000216</td>
      <td>0</td>
      <td>ironic, final notornis are fluffily across the...</td>
    </tr>
    <tr>
      <th>17</th>
      <td>199</td>
      <td>52970</td>
      <td>O</td>
      <td>112986.49</td>
      <td>1996-03-07</td>
      <td>2-HIGH</td>
      <td>Clerk#000000489</td>
      <td>0</td>
      <td>unusual, regular requests c</td>
    </tr>
    <tr>
      <th>18</th>
      <td>227</td>
      <td>9883</td>
      <td>O</td>
      <td>54880.58</td>
      <td>1995-11-10</td>
      <td>5-LOW</td>
      <td>Clerk#000000919</td>
      <td>0</td>
      <td>asymptotes are special, special requests. spec</td>
    </tr>
    <tr>
      <th>19</th>
      <td>231</td>
      <td>90818</td>
      <td>F</td>
      <td>191808.17</td>
      <td>1994-09-29</td>
      <td>2-HIGH</td>
      <td>Clerk#000000446</td>
      <td>0</td>
      <td>express requests use always at the unusual dep...</td>
    </tr>
  </tbody>
</table>
</div>



The same works in reverse to load local files 


```python
with con.cursor() as cursor:
    cursor.execute('''
        insert into tpch..orders
            select * from external '/tmp/orders.csv'
                using (
                    delim '|' 
                    remotesource 'python'
                    skiprows 1)''')
    print(f"{cursor.rowcount} rows inserted")
    # cursor.rowcount will report the number of rows loaded
```

    20 rows inserted


#### Loading from other data sources

Data sources, like external servers, github etc can be fit into this model by streaming data from the source through the python application itself. The python using `nzpy` can read data from external sources and connect that directly to `nzpy` pipe via a named pipe. 

_(Note: The named pipe method below works seamlessly on Linux and Mac. For windows win32pipe module can be used to achieve the same)_

Creating a pipe for all data streaming loads lets one have a thread that can push data to this pipe. The other end is connected to Netezza via an external table.



```python
import pathlib
datapipe = pathlib.Path("/tmp/datapipe")

def create_datapipe():
    global datapipe
    if datapipe.exists():
        datapipe.unlink()
    os.mkfifo(datapipe)
```

The `requests` module coupled with `shutil.copyobject` can be used to actually stream rather than spool data. Here's an example of streaming data that is obtained as gzip'd from github


```python
import requests, shutil, gzip

def load_published_dataset(ds):
    with open(datapipe, "wb") as pipe:
        with requests.get(ds, stream=True) as r:
            with gzip.GzipFile(fileobj=r.raw) as unzip:
                shutil.copyfileobj(unzip, pipe)

```

Connecting the two together, and creating an external table over the named pipe will like this - 

_(Note: the pipe is put in a separate thread so that nzpy doesn't block the data stream)_

The `Cursor.rowcount` attribute will report the number of rows loaded


```python
import threading

create_datapipe()

source = 'https://raw.githubusercontent.com/ibm-watson-data-lab/open-data/master/cars/cars.csv'
streamer = threading.Thread(target=load_published_dataset, args=(source,))
streamer.start()

with con.cursor() as cursor:                            
    cursor.execute(f''' 
        insert into cars select * from external '{datapipe}' 
            using (
                delim ','
                remotesource 'python'
                skiprows 1  -- skip the header
            )''')
    print(f"{cursor.rowcount} rows inserted")
    
streamer.join()


```

    406 rows inserted


Doing it in revserse and specifying a file instead of named pipe will unload the data. `pandas.Dataframe` can be used to directly read the output of a query and then do further analytics on it. 

## A real life example

In this example lets use Python and Netezza Performance Server, to load and analyze the data on [Australian temperatures and rainfall published publically](https://github.com/rfordatascience/tidytuesday/blob/master/data/2020/2020-01-07/readme.md) 

The best practices are 

- Load data into Netezza 
- Do as most of the filtering, transformation and analytics in database
- Do the last step of visualizing and final analytics by extracting the smaller result of the above step in Python

### Step 1 - Dataset

Lets look at the dataset. For the first go around, setup the tables. The data here represents the [Australian weather station temperature and rainfall data as of Jan 1 2020](https://github.com/rfordatascience/tidytuesday/blob/master/data/2020/2020-01-07/readme.md) 

There are two tables 

**temperature.csv**

|column    |type     |description |
|:-----------|:---------|:-----------|
|city_name   |VARCHAR(20) | City Name|
|date        |DATE    | Date |
|temperature |NUMERIC(8,3)    | Temperature in Celsius |
|temp_type   |VARCHAR(10) | Temperature type (min/max daily) |
|site_name   |VARCHAR(30) | Actual site/weather station|

**rainfall.csv**

|column     |type     |description |
|:------------|:---------|:-----------|
|station_code |INT | Station Code |
|city_name    |VARCHAR(20) | City Name |
|year         |INT    | Year |
|month        |INT  | Month |
|day          |INT | Day |
|rainfall     |NUMERIC(8,3)    | rainfall in millimeters|
|period       |INT    | how many days was it collected across |
|quality      |VARCHAR(5) | Certified quality or not |
|lat          |NUMERIC(5,2)    | latitude |
|long         |NUMERIC(5,2)    | longitude |
|station_name |VARCHAR(30) | Station Name |


### Step 2 - Setup the tables


```python
with con.cursor() as cursor:
    # find which of the tables already exist
    existing = { table[0] for table in cursor.execute("select lower(tablename) from _v_table where lower(tablename) in (?, ?)",
                                                 ("rainfall", "temperature")).fetchall() }
    if 'temperature' not in existing:
        # create only if they don't
        cursor.execute('''
        create table temperature (
            city_name     VARCHAR(20),
            date          DATE,
            temperature   NUMERIC(8,3),
            temp_type     VARCHAR(10),
            site_name     VARCHAR(30)
        ) distribute on (city_name)
        ''')
        print("Table temperature created")
        
    if 'rainfall' not in existing:
        cursor.execute('''
        create table rainfall (
            station_code  INT,
            city_name     VARCHAR(20),
            year          INT,
            month         INT,
            day           INT,
            rainfall      NUMERIC(8,3),
            period        INT,
            quality       VARCHAR(5),
            lat           NUMERIC(5,2),
            long          NUMERIC(5,2),
            station_name  VARCHAR(100)
        ) distribute on (city_name, station_code)
        ''')
        print("Table rainfall created")
```

Now stream and load both tables 


```python
create_datapipe()

rainfall = "https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2020/2020-01-07/rainfall.csv"
temperature = "https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2020/2020-01-07/temperature.csv"

loader = threading.Thread(target=load_published_dataset, args=(rainfall,))
loader.start()

with con.cursor() as cursor:
    print("Loading data", end=".. ")
    result = cursor.execute(f'''INSERT INTO rainfall SELECT * FROM EXTERNAL '{datapipe}'
                                USING (
                                    DELIMITER ','
                                    REMOTESOURCE 'python'
                                    NULLVALUE 'NA'
                                    SKIPROWS 1)''')
    print(f"{cursor.rowcount} rows inserted")
    
loader.join()
```

    Loading data.. 179273 rows inserted



```python
create_datapipe()
temperature = "https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2020/2020-01-07/temperature.csv"

loader = threading.Thread(target=load_published_dataset, args=(temperature,))
loader.start()

with con.cursor() as cursor:
    print("Loading data", end=".. ")
    result = cursor.execute(f'''INSERT INTO temperature SELECT * FROM EXTERNAL '{datapipe}'
                                USING (
                                    DELIMITER ','
                                    REMOTESOURCE 'python'
                                    NULLVALUE 'NA'
                                    SKIPROWS 1)''')
    print(f"{cursor.rowcount} rows inserted")
    
loader.join()
```

    Loading data.. 528278 rows inserted


### Step - 3 Analyze the data

First analyze the data in database by grouping the temperatures across months and decades to reduce the dataset. After that visualization and further analytics can be done easily in python


```python
df = pd.read_sql('''
    select extract(decade from date) * 10 as decade,
           extract(month from date) as month,
           city_name,
           avg(temperature) as avg
        from temperature
        where city_name in ('SYDNEY', 'MELBOURNE')
        group by month, decade, city_name
        order by month
 ''', con)

df.columns = [c.decode().lower() for c in df.columns]
df.decade = df.decade.astype(int)
df.avg = df.avg.astype(float)
df.month = df.month.astype(int)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>decade</th>
      <th>month</th>
      <th>city_name</th>
      <th>avg</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1910</td>
      <td>1</td>
      <td>MELBOURNE</td>
      <td>19.161613</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1910</td>
      <td>1</td>
      <td>SYDNEY</td>
      <td>23.140777</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1920</td>
      <td>1</td>
      <td>MELBOURNE</td>
      <td>18.792419</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1920</td>
      <td>1</td>
      <td>SYDNEY</td>
      <td>22.192581</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1930</td>
      <td>1</td>
      <td>MELBOURNE</td>
      <td>19.118710</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>259</th>
      <td>1990</td>
      <td>12</td>
      <td>SYDNEY</td>
      <td>21.864194</td>
    </tr>
    <tr>
      <th>260</th>
      <td>2000</td>
      <td>12</td>
      <td>MELBOURNE</td>
      <td>19.038871</td>
    </tr>
    <tr>
      <th>261</th>
      <td>2010</td>
      <td>12</td>
      <td>MELBOURNE</td>
      <td>19.754480</td>
    </tr>
    <tr>
      <th>262</th>
      <td>2000</td>
      <td>12</td>
      <td>SYDNEY</td>
      <td>22.285737</td>
    </tr>
    <tr>
      <th>263</th>
      <td>2010</td>
      <td>12</td>
      <td>SYDNEY</td>
      <td>22.547849</td>
    </tr>
  </tbody>
</table>
<p>264 rows × 4 columns</p>
</div>



Now we can combine `DataFrame` with `ggplot` too see how average temperatures for two cities across the year stack up across all decades for the last 100 years


```python
import matplotlib, calendar
from plotnine import *
%matplotlib inline

( ggplot(df, aes(x='month', y='avg', group='decade', color='decade')) + geom_line() + geom_point() + 
  labs(y = "Average Temperature (°C)", x = "Month") + facet_wrap('city_name') +
   scale_color_gradient(low="blue", high="red") + 
   scale_x_discrete(labels=list(calendar.month_abbr[1:]), limits=range(12)) + 
   theme(axis_text_x=element_text(rotation=60, hjust=1))
)
```


![png](output_23_0.png)


