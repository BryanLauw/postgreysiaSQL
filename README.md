# Mega Project IF3140 Database System
> by PostgreysiaSQL

## Table of Contents
* [Project Description](#project-description)
* [Setup and Usage](#setup-and-usage)

## Project Description
This project aims to create a mini DBMS. The DBMS can read and modify existing tables. The DBMS components include a query optimizer, storage manager, failure recovery, concurrency control, and query processor. The DBMS has mechanisms to optimize queries, execute multiple transactions simultaneously, and recover from errors. The DBMS is limited to reading and updating data.

## Setup and Usage
1. Install Python 3.12
2. Install source code DBMS from tag release v4.1
3. Run server by `python server.py database1`
4. Run a client or more in different command prompt by `python client.py`
5. Insert your query