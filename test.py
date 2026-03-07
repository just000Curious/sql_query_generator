from db_information import *
from pypika_query_engine import *
db = DBInfo(df, "public.gmhk_appointment")
df = ("db_files/extracted_gm_schema.csv")

generator = QueryGenerator(db)

query = (
    generator
    .select(["emp_no", "appointment_date"])
    .where("emp_no", ">", 100)
    .build()
)

print(query)
