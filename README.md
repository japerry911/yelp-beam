# Yelp Beam (Sky Beam)


### Command to Trigger 
python -m sky_beam.main \
--runner DataflowRunner \
--folder_name <RAW-DATA-FOLDER-PREFIX> \
--temp_location <GCS-BUCKET-LOCATION> \
--project <PROJECT-ID> \
--region <REGION>

### Stack
  
- technologies used:
  - Python
  - Apache Beam
  - Google Cloud Dataflow
  - Google Cloud BigQuery
  - Google Cloud Storage
  - GitHub Actions (black formatter, isort, flake8, GitGuardian community action)
  
### Accompanying Blog post located [here](https://jackskylord.medium.com/apache-beam-exploration-50905f1c16b8)
