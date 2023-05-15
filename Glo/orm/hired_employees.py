from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

class HiredEmployee(Base):
    __tablename__ = 'hired_employees'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name_ = Column(String(255), nullable=False)
    datetime_ = Column(DateTime, nullable=False)
    department_id = Column(Integer, nullable=False)
    job_id = Column(Integer, nullable=False)

def process_hired_employees(id, name_, datetime_, department_id, job_id):
    employee = HiredEmployee(
        id=id,
        name_=name_,
        datetime_=datetime_,
        department_id=department_id,
        job_id=job_id
    )
    return employee
