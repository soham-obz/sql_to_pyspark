from pyspark.sql.functions import col, dense_rank, lag, avg, percent_rank, lead
from pyspark.sql.window import Window

employees = spark.table("employees")

window_dept_salary = Window.partitionBy("department").orderBy(col("salary").desc())
window_dept_hire_date = Window.partitionBy("department").orderBy("hire_date")
window_salary = Window.orderBy("salary")

result = employees.select(
    col("employee_id"),
    col("department"),
    col("salary"),
    col("hire_date"),
    dense_rank().over(window_dept_salary).alias("dept_salary_rank"),
    (col("salary") - lag("salary").over(window_dept_hire_date)).alias("salary_diff_from_prev_hire"),
    avg("salary").over(window_dept_hire_date.rowsBetween(Window.unboundedPreceding, Window.currentRow)).alias("running_avg_salary"),
    percent_rank().over(window_salary).alias("company_salary_percentile"),
    lead("employee_id").over(window_dept_hire_date).alias("next_hired_employee")
)