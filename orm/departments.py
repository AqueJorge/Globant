from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Department(Base):
    __tablename__ = 'departments'

    id = Column(Integer, primary_key=True)
    department = Column(String(255), nullable=False)

def process_department(id, department):
    department_obj = Department(id=id, department=department)
    return department_obj
