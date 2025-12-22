# DataEng 2024 Template Repository

<img src="./images/logo-insa_0.png" alt="INSALogo" style="width:100%; height:auto; display:block; margin-left:auto; margin-right:auto;"/>
<br>

Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) is provided by [INSA Lyon](https://www.insa-lyon.fr/).

Students: JOUENNE Maia, TRON Baptiste, VIVIER Thibault

### Abstract


## Objectives 

### Learning objectives 

This project is about data eng. 
What we need to learn ? To implement a dataeng pipeline from sources to production data using specifics tools and concept saw in class.
What are the mandatory tools ? Docker and Airflow.

### Our objectives 
Be able to implement a full dateng pipeline = of course. But also know how to use dataeng tools, and understand the context of using of the differents tools used in class. The main objective is to understand and use specifics tools dedicated to specific use case in dataeng. 
We will introduce trhose tools and explain why we choose to use them in our project, their advantage and limitations.
<br>
Also, we consider that this project is a chance to develop our skills in different fields (developpement, system conception, Artificial intelligence, etc.), that's why we choose to create ambitious pipelines with diverse tools and methodology. 

This project is done only with educational purpose.


## Datasets Description 
### Data source introduction
<br>

**Script slug :** https://www.scriptslug.com/scripts/medium/film?sort=az
<br>

Script Slug is a popular online resource for screenwriters, especially those interested in animation and film. The site offers a growing library of original screenplays from major studios like Netflix, HBO, and Marvel, allowing users to study professional scripts for structure, pacing, and dialogue. It’s widely used by aspiring writers to improve their own screenwriting skills by reading and analyzing industry-standard scripts. Script Slug also provides educational tips and resources for animation screenwriting. The scripts are downloadable in pdf.

<br>
<img src="./images/script_slug_website.png" alt="ScriptSlugWebsite" style="width:50%; height:auto; display:block; margin-left:auto; margin-right:auto;"/>

<br><br><br>

**ImsDB :** https://imsdb.com/alphabetical/0
<br>

IMSDb (Internet Movie Script Database) is a well-known online repository offering a vast collection of movie and TV show scripts. It provides free access to original screenplays, making it a valuable resource for screenwriters, film students, and cinephiles who want to study professional writing techniques. The site relies partly on community contributions to expand its library and keep scripts up to date. IMSDb is widely used for learning script structure, dialogue, and storytelling from real industry examples. There, the scripts are available on html pages.

<br>
<img src="./images/imsdb_website.png" alt="ImsDBWebsite" style="width:50%; height:auto; display:block; margin-left:auto; margin-right:auto;"/>
<br><br><br>

**All the tropes (Wiki) :** https://allthetropes.org/wiki
<br>

AllTheTropes is a community-driven wiki dedicated to cataloging and explaining storytelling tropes—recurring themes, devices, and conventions—found in movies, TV shows, books, video games, and other media. Unlike other trope databases, AllTheTropes is open and collaborative, allowing anyone to contribute or edit entries. It serves as a valuable resource for writers, critics, and fans seeking to understand, analyze, or avoid clichés in storytelling. The site is especially useful for exploring how tropes evolve and are used across different genres and cultures.

<br>
<img src="./images/all_the_tropes_website.png" alt="AllTheTropesWebsite" style="width:50%; height:auto; display:block; margin-left:auto; margin-right:auto;"/>
<br><br><br>

**Some vocabulary :** 
<br>

The Cambridge online dictionary define a "Trope" as follow :
Trope, noun : something such as an idea, phrase, or image that is often used in a particular artist's work, in a particular type of art, in a media, etc.	Comparer : cliché.
<br>

Our definition : a trope is a recurring narrative conventions or schema used in storytelling. They are tools used by a writter. Tropes can be applied to almost everything : plot, characters, devices, themes, etc. 
<br>

Example : 
<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Human-like robots is a classic Science Fiction tropes. You can find it in : Ex-Machina or Blade Runner.
<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- The vilan protagonist : a plot which implies that the protagonist followed is or become a vilain among the story. You can find it in : Breaking Bad or Night Call (NightCrawler)

<br>
<img src="./images/poster_quoted_movies.png" alt="PosterQuotedMovies" style="width:50%; height:auto; display:block; margin-left:auto; margin-right:auto;"/>

<br><br><br>

### Purpose

Let's say it, we want to try to build an AI agent able to help a young writter to write his first script. The idea is NOT to automatize the script creation but to provide ideas, example and draft to the user to help him during his writting session.
<br>
It's beautiful, but... it is very ambitious. That's why our first objective is to try to buildt an AI model able to identify properly the tropes used in a movie script.


### Data description

#### Scripts
**PDF**
<br>
On the Scriptslug website, the movie scripts are available on PDF format. Those PDF files are often scanned pages or brut text. To extract the text content of those files, we will use an OCR (Optical Character Recognition) tool. 

<br>
<img src="./images/pdf_script_example.png" alt="PdfScriptExample" style="width:50%; height:auto; display:block; margin-left:auto; margin-right:auto;"/>

<br><br><br>

**HTML**
<br>
On the ImsDB website, the movie scripts are available on HTML pages. It is possible to get the content of those pages, but it requires some cleaning to get the text content, with dedicated tools.

<br>
<img src="./images/html_script_example.png" alt="PdfScriptExample" style="width:50%; height:auto; display:block; margin-left:auto; margin-right:auto;"/>

<br><br><br>

#### Tropes data

## Main tools introduction

### Docker
general presentation : refer to the lectures.
precise the use in this context.

### Airflow
general presentation : refer to the lectures.
precise the use in this context.

### MongoDB
general presentation : refer to the lectures.
precise the use in this context.

## Architecture 

### Global architecture
Describe the general architecture : explain what are the different DAGs, how they interact between each other (shared volume : project_data).
Explain the design choices : why using a scrapping DAG ? How the project_data volume is structured ? How the dockerc compose is built (which services ? why ?)? What is the project architecture (folder path) ?etc...?


### DAG 1 : scrapping DAG

#### General presentation

**logical schema img**

Our first Airflow DAG is dedicated to scrap information about the data from sources to prepare the data ingestion. This DAG allow us to get list of links, names, and merge information of the different data sources to define the range of the data ingestion (to avoid to scrap useless data, something essential regarding the cost of execution - time). This scrapping DAG requires specific tools which are Selenium and Chrome Browser. 
<br>
Due to those particular requirements (and also because it's challenging), we decide to use a DockerOperator in Airflow to launch a Container dedicated to the scrapping. The data scrapped are then send to a Docker Volume : project_data, shared with the other DAGs (to be able to access to the data from the different DAGs).

#### Specific tools 
Selenium.

ChromeBrowser : need to be installed to use Selenium.


#### Detailled operations
Let's have a look on each steps...

This is where you precise the operation of each operator and the specifity. 
=> why this step is useful ? what is the purpose of this one ?


#### Difficulties
What was the hardiest things ? Why ? How we surpass them ?
=> volume management, DockerOperator complexity.




### DAG 2 : ingestion DAG

#### General presentation

**logical schema img**

Present fastly what the DAG is doing, which specific tools are used and what are the specificity of this DAG ?


#### Specific tools 
Selenium.

ChromeBrowser : need to be installed to use Selenium.


#### Detailled operations
Let's have a look on each steps...


#### Difficulties
What was the hardiest things ? Why ? How we surpass them ?




## Queries 

## Requirements

## Note for Students

* Clone the created repository offline;
* Add your name and surname into the Readme file and your teammates as collaborators
* Complete the field above after project is approved
* Make any changes to your repository according to the specific assignment;
* Ensure code reproducibility and instructions on how to replicate the results;
* Add an open-source license, e.g., Apache 2.0;
* README is automatically converted into pdf

