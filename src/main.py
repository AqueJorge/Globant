from fastapi import FastAPI, UploadFile, File, Response
from dotenv import load_dotenv
import os
from orm.hired_employees import process_hired_employees, HiredEmployee
from orm.departments import process_department, Department
from orm.jobs import process_job, Job
from orm.database import SessionLocal
import logging
from datetime import datetime
import shutil
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter


app = FastAPI()

backup_dir = r"PATH FOR THE BACKUPS FILES"  # Ruta de la carpeta backup

# Cargar variables de entorno
dotenv_path = os.path.join(os.path.dirname(__file__), '..', 'config', '.env')
load_dotenv(dotenv_path)

#logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
file_handler = logging.FileHandler("error_log.txt")
file_handler.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

allowed_filenames = ["hired_employees.csv", "departments.csv", "jobs.csv"]

@app.post("/uploadfile/")
async def root(file: UploadFile = File(...)):
    if file.filename in allowed_filenames:
        try:

            process_upload_file(file.file, file.filename)

            return {"file_name": file.filename}
        except Exception as e:
            return {"message": f"Error occurred while saving data from {file.filename}: {str(e)}"}
    else:
        return {"message": "Invalid file name"}


@app.get("/backup/")
async def backup_data(response: Response):
    # Obtener la fecha actual
    current_date = datetime.now().strftime("%d%m%Y%H%M%S")
    # Crear carpeta de backup si no existe
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)

    hired_employees_schema = avro.schema.Parse('''
        {
            "type": "record",
            "name": "HiredEmployee",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name_", "type": "string"},
                {"name": "datetime_", "type": "string"},
                {"name": "department_id", "type": "int"},
                {"name": "job_id", "type": "int"}
            ]
        }
    ''')

    departments_schema = avro.schema.Parse('''
        {
            "type": "record",
            "name": "Department",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "department", "type": "string"}
            ]
        }
    ''')

    jobs_schema = avro.schema.Parse('''
        {
            "type": "record",
            "name": "Job",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "job", "type": "string"}
            ]
        }
    ''')

    #backup de hired_employees
    session = SessionLocal()
    hired_employees = session.query(HiredEmployee).all()
    hired_employees_file = os.path.join(backup_dir, f"hired_employees_{current_date}.avro")
    with open(hired_employees_file, 'wb') as avro_file:
        writer = DataFileWriter(avro_file, DatumWriter(), hired_employees_schema)
        for employee in hired_employees:
            employee_dict = employee.__dict__
            employee_dict.pop('_sa_instance_state', None)
            writer.append(employee_dict)
        writer.close()


    #backup de departments
    departments = session.query(Department).all()
    departments_file = os.path.join(backup_dir, f"departments_{current_date}.avro")
    with open(departments_file, 'wb') as avro_file:
        writer = DataFileWriter(avro_file, DatumWriter(), departments_schema)
        for department in departments:
            department_dict = department.__dict__
            department_dict.pop('_sa_instance_state', None)
            writer.append(department_dict)
        writer.close()

    #backup de jobs
    jobs = session.query(Job).all()
    jobs_file = os.path.join(backup_dir, f"jobs_{current_date}.avro")
    with open(jobs_file, 'wb') as avro_file:
        writer = DataFileWriter(avro_file, DatumWriter(), jobs_schema)
        for job in jobs:
            job_dict = job.__dict__
            job_dict.pop('_sa_instance_state', None)
            writer.append(job_dict)
        writer.close()


    #archivos AVRO en un archivo ZIP
    backup_zip_file = os.path.join(backup_dir, "backup.zip")
    shutil.make_archive(backup_zip_file[:-4], "zip", backup_dir)

    return {"message": "Backup completed successfully."}


