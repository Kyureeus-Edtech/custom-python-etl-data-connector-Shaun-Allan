# etl/pipeline.py
from . import extract, transform, load
from config import settings
import logging

def run_full_etl():
    """
    Runs the complete ETL pipeline for all projects.
    """
    logging.info("Starting SonarQube ETL Pipeline...")

    # 1. EXTRACT Projects
    raw_projects = extract.get_projects()
    if not raw_projects:
        logging.warning("No projects found. Exiting pipeline.")
        return

    # 2. TRANSFORM and LOAD Projects
    transformed_projects = transform.transform_project_data(raw_projects)
    load.bulk_upsert(settings.MONGO_PROJECTS_COLLECTION, transformed_projects, 'project_key')
    
    # 3. For each project, extract, transform, and load its issues and measures
    for project in transformed_projects:
        project_key = project['project_key']

        # --- Process Issues ---
        raw_issues = extract.get_issues_for_project(project_key)
        transformed_issues = transform.transform_issue_data(raw_issues)
        load.bulk_upsert(settings.MONGO_ISSUES_COLLECTION, transformed_issues, 'issue_key')

        # --- Process Measures ---
        raw_measures = extract.get_measures_for_project(project_key)
        if raw_measures:
            transformed_measures = transform.transform_measures_data(raw_measures)
            if transformed_measures:
                # Since measures are a single document per project, we wrap it in a list
                load.bulk_upsert(settings.MONGO_MEASURES_COLLECTION, [transformed_measures], 'project_key')

    logging.info("SonarQube ETL Pipeline finished successfully.")