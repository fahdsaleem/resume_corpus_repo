# -*- coding: utf-8 -*-
"""
Created on Thu Dec 21 17:30:51 2023

@author: FSA
"""

def test_table_exists():
     """ Test the tables existence

     Arguments:
     n.a: No arguments passed
     """
     assert spark.catalog.tableExists('normalized_classes') is True
     assert spark.catalog.tableExists('resume_corpus') is True
     assert spark.catalog.tableExists('resume_validation') is True

def column_exists(dataFrame, column_name):
     if column_name in dataFrame.columns:
          return True
     else:
          return False

def test_fields_created_updated_at_exists():
     """ Test the metadata columns are present in tables

     Arguments:
     n.a: No arguments passed
     """
     resume_corpus_df = spark.sql('select * from resume_corpus where 1=0')
     assert column_exists(resume_corpus_df,'created_at') is True
     assert column_exists(resume_corpus_df,'modified_at') is True
     
     resume_validation_df = spark.sql('select * from resume_validation where 1=0')
     assert column_exists(resume_validation_df,'created_at') is True
     assert column_exists(resume_validation_df,'modified_at') is True
 
 
def test_extract_tag_info():
     """ Test the extraction function is performing as per requirements

     Arguments:
     n.a: No arguments passed
     """
     from pyspark.sql.types import StructType, StructField, StringType
     from pyspark.testing import assertDataFrameEqual

     # Create test data for the unit tests to run against
     schema = StructType([StructField("resume",  StringType(), True),\
     StructField("job_label",  StringType(), True)])

     data = [ ("Oracle Database Administrator Oracle <span class=\"hl\">Database</span> <span class=\"hl\">Administrator</span> Oracle Database Administrator - Cognizant hyderabad Carrier Objective: To Obtain a Oracle DBA Position in a progressive Company Where I Can Utilise and Enhance my Experience and Knowledge in Constituting Effectively to the Success of the Organization, and also to Improve my Further Technical and Professional Skills. Profile Summary: Over 4+ years of Experience as Architecture.","system admin"),\
     ("Database Administrator / Database Developer <span class=\"hl\">Database</span> <span class=\"hl\">Administrator</span> / <span class=\"hl\">Database</span> Developer Database Administrator / Database Developer - Dominion Diagnostics, LLC Providence, RI Available for contact between 9:00 AM and 9:00 PM. Work Experience Database Administrator / Database Developer Dominion Diagnostics, LLC - North Kingstown, RI April 2019 to PresentManage four different database environments: MS SQL Server","admin") ]
     test_data_df = spark.createDataFrame(data, schema)
     
     # Create expected result data for the unit tests to compare against
     result_schema = StructType([StructField("job_title",  StringType(), True)])
     result_data = [('Database Administrator',),('Database Administrator Database',)]
     result_data_df=spark.createDataFrame(result_data, result_schema)   

     # extract tag info for test dataset
     test_data_df=extract_tag_info(test_data_df,'span' ,'job_title')
     calculated_data_df = test_data_df.select('job_title') 

     # assert data frame equality
     assertDataFrameEqual(result_data_df, calculated_data_df)

def test_resume_validation():
    """ Test the resume_validation table record counts by comparing with resume_corpus_count

    Arguments:
    n.a: No arguments passed
    """
    from pyspark.testing import assertDataFrameEqual
    resume_corpus_count=spark.sql('select count(*) as count from resume_corpus')
    resume_validation_count=spark.sql('select count(*) as count from resume_validation')
    assertDataFrameEqual(resume_corpus_count, resume_validation_count)