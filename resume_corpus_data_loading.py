# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 19:27:59 2023

@author: FSA
"""

def create_delta_tables():
    """ Create the normalized_classes, resume_corpus and resume_validation according to the specifications

    Arguments:
    n.a: No arguments passed
    """
    spark.sql('''
    create or replace table normalized_classes (
    job_titles stringg,
    job_label string
    )
    using delta 
    ''') 

    spark.sql('''
    create or replace table resume_corpus (
    resume_id bigint not null generated always as identity,
    job_title string,
    job_label string,
    resume string,
    created_at timestamp,
    modified_at timestamp
    )
    using delta
    partitioned by(job_label)
    ''') 

    spark.sql('''
    create or replace table resume_validation (
    resume_id bigint,
    validation_outcome boolean,
    created_at timestamp,
    modified_at timestamp
    )
    using delta
    ''') 
    
def load_normalized_classes():
    """ Load the normalized_classes table using text file with : delimiter according to the specifications

    Arguments:
    n.a: No arguments passed
    """
    normalized_classes_sourcefile ='file:/Workspace/Repos/fahd.saleem@gmail.com/resume_corpus/normlized_classes.txt'
    # read normalized classes 
    normalized_classes_df= spark.read.option('delimiter', ':').csv(normalized_classes_sourcefile)
    # naming dataframe fields to required field names  
    normalized_classes_df = normalized_classes_df.withColumnRenamed('_c0', "job_titles").withColumnRenamed('_c1', "job_label")
    # saving data in normalized_classes delta table 
    normalized_classes_df.write.mode('overwrite').format('delta').saveAsTable('normalized_classes')
    # z-ordering table on job_label field
    spark.sql('optimize normalized_classes zorder by (job_label)')
    # viewing data loaded in delta table
    display(spark.sql('select * from normalized_classes '))
    
def load_resume_corpus():
    """ Load the resume_corpus partitioned table using .lab and .txt file. 

    Arguments:
    n.a: No arguments passed
    """
    from pyspark.sql.functions import input_file_name, split, reverse, regexp_replace, regexp_extract_all,lit,concat_ws,current_timestamp
    resume_corpus_labels_sourcefiles ='dbfs:/resumes_corpus/*.lab'
    resume_corpus_sourcefiles ='dbfs:/resumes_corpus/*.txt'
    #resume_corpus_labels_sourcefiles='file:/Workspace/Users/fahd.saleem@gmail.com/*.lab'
    #resume_corpus_sourcefiles='file:/Workspace/Users/fahd.saleem@gmail.com/*.txt'
    # read resume corpus labels
    resume_corpus_labels_df= spark.read.csv(resume_corpus_labels_sourcefiles)

    # naming dataframe fields to required field names  
    resume_corpus_labels_df = resume_corpus_labels_df.withColumnRenamed('_c0', 'job_label')

    # creating filename joining column  such that 00001.txt translates to 00001
    resume_corpus_labels_df = resume_corpus_labels_df.withColumn('input_file', input_file_name())
    resume_corpus_labels_df = resume_corpus_labels_df.withColumn('file', regexp_replace(reverse(split('input_file','/')).getItem(0),'.lab',''))

    # read resume corpus text files
    resume_corpus_df= spark.read.text(resume_corpus_sourcefiles)

    # naming dataframe fields to required field names  
    resume_corpus_df = resume_corpus_df.withColumnRenamed('value', 'resume')

    # creating filename joining column  such that 00001.txt translates to 00001
    resume_corpus_df = resume_corpus_df.withColumn('input_file', input_file_name())
    resume_corpus_df = resume_corpus_df.withColumn('file', regexp_replace(reverse(split('input_file','/')).getItem(0),'.txt',''))
    
    # join labels and resume_corpus
    resume_corpus_final_df=resume_corpus_df.join(resume_corpus_labels_df,resume_corpus_df.file == resume_corpus_labels_df.file,'outer').select(resume_corpus_labels_df.job_label,resume_corpus_df.resume)
    # display(resume_corpus_final_df)

    # extracting job titles from resume fields
    tag = 'span'
    pattern = '<'+tag+'(.+?)>(.+?)<\\/'+tag+'>'
    resume_corpus_final_df=resume_corpus_final_df.withColumn('job_title',concat_ws(' ',regexp_extract_all(resume_corpus_final_df.resume,lit(pattern),2)))
    resume_corpus_final_df=resume_corpus_final_df.withColumn('created_at',current_timestamp() )
    #display(resume_corpus_final_df)
    # saving data in resume_corpus delta table  
    resume_corpus_final_df.write.mode('overwrite').format('delta').saveAsTable('resume_corpus')
    # viewing data loaded in delta table
    display(spark.sql('select * from resume_corpus '))

def load_resume_validation():
    """ Load the resume_validation table according to the specifications

    Arguments:
    n.a: No arguments passed
    """
    from pyspark.sql.functions import current_timestamp
    #join resume_corpus with normalized_classes based on job_title and job_label fields and tag the matched resumes as true.
    resume_validation_df=spark.sql('select resume_id, case when b.job_label is not null THEN true ELSE false END as validation_outcome from resume_corpus a left join normalized_classes b on a.job_title=b.job_label ')
    resume_validation_df=resume_validation_df.withColumn('created_at',current_timestamp() )
    # saving data in resume_corpus delta table  
    resume_validation_df.write.mode('overwrite').format('delta').saveAsTable('resume_validation')
    display(spark.sql('select * from resume_validation '))

def main():
    try:    
        import logging

        step='1'
        logging.info('starting delta tables creation....')
        create_delta_tables()
        logging.info('delta tables created successfully')
        
        step='2'
        logging.info('starting normalized classes loading....')
        load_normalized_classes()
        logging.info('normalized classes loading completed successfully')
        
        step='3'
        logging.info('starting resume corpus loading....')
        load_resume_corpus()
        logging.info('resume corpus loading completed successfully')
        
        step='4'
        logging.info('starting resume validation loading....')
        load_resume_validation()
        logging.info('resume validation loading completed successfully')
        
        step='5'
        logging.info('Data loading process completed')
        
    except Exception as e:  
        output = f"Error:{e}"
        logging.error('Data processing halted at step '+step+'.'+output)
        raise Exception ('Data processing halted at step '+step+'.'+output)
if __name__ == "__main__":
    main()
    
