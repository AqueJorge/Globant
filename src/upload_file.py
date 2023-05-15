from orm.hired_employees import process_hired_employees
from orm.departments import process_department
from orm.jobs import process_job
from orm.database import SessionLocal

def save_employees_to_db(employees):
    db_session = SessionLocal()
    try:
        for emp_data in employees:
            if all(value != '' for value in emp_data.values()):
                employee = process_hired_employees(**emp_data)
                db_session.add(employee)
        db_session.commit()
    except Exception as e:
        db_session.rollback()
        raise
    finally:
        db_session.close()

def save_departments_to_db(departments):
    db_session = SessionLocal()
    try:
        for dept_data in departments:
            if all(value != '' for value in dept_data.values()):
                department = process_department(**dept_data)
                db_session.add(department)
        db_session.commit()
    except Exception as e:
        db_session.rollback()
        raise
    finally:
        db_session.close()

def save_jobs_to_db(jobs):
    db_session = SessionLocal()
    try:
        for job_data in jobs:
            if all(value != '' for value in job_data.values()):
                job = process_job(**job_data)
                db_session.add(job)
        db_session.commit()
    except Exception as e:
        db_session.rollback()
        raise 
    finally:
        db_session.close()

def process_upload_file(file, filename):
    if filename == "hired_employees.csv":
        employees = []
        for line in file:
            line = line.decode().strip().split(',')
            employee_data = {
                'id': int(line[0]) if line[0] else 0,
                'name_': line[1],
                'datetime_': line[2],
                'department_id': int(line[3]) if line[3] else 0,
                'job_id': int(line[4]) if line[4] else 0
            }
            employees.append(employee_data)
        save_employees_to_db(employees)
    elif filename == "departments.csv":
        departments = []
        for line in file:
            line = line.decode().strip().split(',')
            department_data = {
                'id': int(line[0]) if line[0] else 0,
                'department': line[1]
            }
            departments.append(department_data)
        save_departments_to_db(departments)
    elif filename == "jobs.csv":
        jobs = []
        for line in file:
            line = line.decode().strip().split(',')
            job_data = {
                'id': int(line[0]) if line[0] else 0,
                'job': line[1]
            }
            jobs.append(job_data)
        save_jobs_to_db(jobs)
