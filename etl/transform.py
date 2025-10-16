# etl/transform.py
import logging

def transform_project_data(projects):
    """
    Transforms raw project data into a cleaner format.
    Example: Select specific fields.
    """
    logging.info(f"Transforming {len(projects)} projects...")
    transformed = []
    for project in projects:
        transformed.append({
            'project_key': project.get('key'),
            'name': project.get('name'),
            'qualifier': project.get('qualifier'),
            'visibility': project.get('visibility')
        })
    return transformed

def transform_issue_data(issues):
    """
    Transforms raw issue data.
    Example: Add a calculated field for 'is_critical'.
    """
    logging.info(f"Transforming {len(issues)} issues...")
    transformed = []
    for issue in issues:
        severity = issue.get('severity')
        transformed.append({
            'issue_key': issue.get('key'),
            'project_key': issue.get('project'),
            'component': issue.get('component'),
            'line': issue.get('line'),
            'message': issue.get('message'),
            'severity': severity,
            'type': issue.get('type'),
            'status': issue.get('status'),
            'creation_date': issue.get('creationDate'),
            # **This is your "complex transformation" example**
            'is_critical_or_blocker': severity in ['CRITICAL', 'BLOCKER']
        })
    return transformed

def transform_measures_data(measures_data):
    """
    Transforms measures data by flattening the nested structure.
    """
    if not measures_data or 'measures' not in measures_data:
        return None
    
    logging.info(f"Transforming measures for project: {measures_data.get('key')}")
    
    flattened_measures = {
        'project_key': measures_data.get('key'),
        'project_name': measures_data.get('name')
    }
    
    for measure in measures_data['measures']:
        metric_key = measure['metric']
        metric_value = measure.get('value', 0)
        # Attempt to convert value to a number if possible
        try:
            flattened_measures[metric_key] = float(metric_value)
        except (ValueError, TypeError):
            flattened_measures[metric_key] = metric_value
            
    return flattened_measures