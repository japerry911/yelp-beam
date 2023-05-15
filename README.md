# Yelp Beam (Sky Beam)


### Command to Trigger 
python -m sky_beam.main \
--runner DataflowRunner \
--folder_name <RAW-DATA-FOLDER-PREFIX> \
--temp_location <GCS-BUCKET-LOCATION> \
--project <PROJECT-ID> \
--region <REGION>>

### Accompanying Blog post

## Introduction

This blog post is an exploration into Apache Beam, specifically into the Google Cloud Dataflow runner. I will discuss what Apache Beam is and also will discuss a Dataflow example that I build as well. Let’s Dive In!
The GitHub for this project is located [here](https://github.com/japerry911/yelp-beam)

## What is Apache BEAM?

Apache Beam is a powerful framework that simplifies the development of data processing pipelines. It enables developers to write code once and execute it on multiple execution engines, making it highly flexible. The main concept behind Apache Beam is the idea of a "unified programming model," which provides a consistent way to express data transformations and operations across different execution environments.

With Apache Beam, developers can focus on the logic of their data processing tasks without being tied to a specific execution engine or programming language. It abstracts away the complexities of distributed computing, allowing users to leverage the capabilities of various execution engines without having to rewrite their code. This portability is achieved through a set of language-specific SDKs (Software Development Kits) that provide a consistent API for data processing across different languages, including Java, Python (Python is used in referenced example), and GoLang.

Apache Beam supports a wide range of data processing patterns, including batch processing and stream processing. It offers a set of built-in transformations and operations, such as filtering, aggregating, joining, and windowing, which in turn makes it easier to express complex data processing tasks. Additionally, it supports both bounded and unbounded data sources, allowing seamless integration with different types of data streams and batch data sets.

By leveraging the capabilities of Apache Beam, developers can build scalable, reliable, and fault-tolerant data pipelines that can process large volumes of data efficiently. Whether it's analyzing real-time data streams, performing ETL (Extract, Transform, Load) operations on batch data, or running complex data processing workflows, Apache Beam provides a unified and powerful framework to handle a variety of data processing challenges.

## Dataflow Example: Sky-Beam

Google Cloud Dataflow is a fully managed service provided by Google Cloud Platform (GCP) that allows you to build and execute data processing pipelines. It is based on the Apache Beam programming model, offering a serverless and scalable solution for processing large volumes of data.
Sky-Beam is an example project that I put together that uses a data dump from Yelp, which includes data from Businesses on Yelp, Check In data from Yelp, and Reviews data from Yelp. This pipeline does the following:
Reads the data (JSON files that range from 100MB to 5GB)
Parses the data into JSON
Flattens the data so that it is all on one level, as opposed to being nested
Writes to BigQuery table (creates the table if needed)
Let’s dissect one of the data pipeline processes within Sky-Beam, specifically the Review data. 

The process begins by utilizing the p context, which represents a beam.Pipeline object. The pipeline utilizes the | and >> operators to execute comments and step-by-step operations. In the first step, the beam.io.ReadFromText method is used to read the data into a PCollection. A PCollection is an immutable distributed collection of data elements that represents the input or output of a data processing pipeline in Apache Beam. These PCollection elements are passed through subsequent pipeline steps.
Next, the PCollection is parsed into JSON using the Python method json.loads, which is called within a beam.ParDo class. beam.ParDo is an Apache Beam transformation that applies a user-defined function to each element of a PCollection, producing zero or more output elements. It enables parallel processing, which is why it is utilized in this and future steps.
The subsequent step involves the "Flatten Parse ParDo," which flattens nested JSON to a single JSON level. This step employs a custom-written process method within a ParDo class.
In the third step, all values are converted to strings using another ParDo class. This choice is made because the data is being landed, and future transformations will be performed within the Transform step. Additionally, this conversion simplifies the schemas and the BigQuery load job for this proof-of-concept (POC) project.
Finally, the pipeline writes the data to BigQuery using the built-in beam.io.WriteToBigQuery method.

## Conclusion

In conclusion, Apache Beam is a versatile and powerful framework that simplifies the development of data processing pipelines. With its unified programming model, developers can write code once and run it on various execution engines, providing flexibility and portability. By abstracting away the complexities of distributed computing, Apache Beam allows users to focus on their data processing logic rather than infrastructure management. Its support for batch and stream processing, along with a rich set of transformations and operations, makes it suitable for a wide range of data processing and analytics use cases (batch example shown in this post). Overall, this was a fun exploration and it was beneficial to develop Sky-Beam within Google Dataflow. When making a decision regarding a data processing framework, one should definitely consider Apache Beam. Thank you for reading, and Happy Coding!
