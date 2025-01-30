def read_sql_file(filename, directory='//opt//airflow//dags//dti-projeto-dicom-gcp//sql//'):
     with open(f'{directory}{filename}', 'r') as s:
         sql_script = s.read()

     return sql_script


