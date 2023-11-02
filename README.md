# BigData-HW01
## Class : BigData Mining And Applications 321524 at NTUT
## By: [Yasar Nazzarian](https://github.com/Yasar2019) 1120120349 IEECS and [郭柏辰 PoChenKuo](https://github.com/PoChenKuo) 112599003 資工博一/computer science first-year PhD student
----------------------------------------------------------------------------------------------
### **Cluser Info**
![Alt text](image.png)
![Alt text](image-1.png)
![Alt text](image-2.png)
### 1. Project Setup and Information
#### 1.1. Environment
* **Python 3.11.4**
* **Java:**
``openjdk version "17.0.1" 2021-10-19
OpenJDK Runtime Environment Temurin-17.0.1+12 (build 17.0.1+12)
OpenJDK 64-Bit Server VM Temurin-17.0.1+12 (build 17.0.1+12, mixed mode, sharing)``
* **Spark 3.5.0**
* **Scala 3.3.1**

#### 1.2. Input

* **Input file:** [spacenews-202309.csv](spacenews-202309.csv)
* **Input file size:** 80.6 MB
* **Input file format:** csv

#### 1.3. Output
#### You can find all the output files in the folder named "output"
* **Output files:** [output](output)
* **Output files format:** csv
* **Task 1:** [First subtask](output/total_word_counts.csv), [Second subtask](output/per_date_word_counts.csv)
* **Task 2:** [First subtask](output/total_content_word_counts.csv), [Second subtask](output\per_date_content_word_counts.csv)
* **Task 3:** [First subtask](output/date_percentage.csv), [Second subtask](output\date_author_percentage.csv)
* **Task 4:** [Output](output\space_in_title_and_postexcerpt.csv)

#### 1.4. Code
* **Code:** [HW01.py](HW01.py)

### 2. How to run the code
```spark-submit --master spark://96.9.210.170:7077 HW01.py```
