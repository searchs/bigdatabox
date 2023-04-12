/***

Different ways to get the Nth  value - using SQL
e.g. get the 3rd highest salary by department given employee and department tables

Options:
NTH_VALUE
ROW_NUMBER()
RANK()
DENSE_RANK()

**/
-- Using Nth Value - by department
SELECT e.first_name,
        e.last_name,
        d.department_name,
        salary,
        Nth_VALUE (salary, 3) OVER (
            PARTITION BY department_name 
            ORDER BY salary DESC 
            RANGE BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING ) AS third_highest_salary
        FROM department d JOIN employee e
        ON d.id = e.department_id; 


-- Using Row Number
SELECT e.first_name,
        e.last_name,
        d.department_name,
        salary,
        ROW_NUMBER() OVER (PARTITION BY d.id ORDER BY salary DESC) AS salary_rank
    FROM department d JOIN employee e
    ON d.id = e.department_id
    ORDER BY department_name;


-- Using CTE
WITH salaries_ranks AS (
SELECT e.first_name,
        e.last_name,
        d.department_name,
        salary,
        ROW_NUMBER() OVER (
            PARTITION BY d.id
            ORDER BY salary DESC) AS salary_rank
FROM department d JOIN employee e 
ON d.id = e.department_id
)

SELECT * FROM salaries_ranks 
WHERE salary_rank = 3;


-- Using RANK()

SELECT e.first_name,
        e.last_name,
        d.department_name,
        salary,
        RANK() OVER (
            PARTITION BY d.department_name
            ORDER BY salary DESC) AS salary_rank
FROM department d JOIN employee e 
ON d.id = e.department_id

-- Using DENSE_RANK() window function
SELECT e.first_name,
        e.last_name,
        d.department_name,
        salary,
        DENSE_RANK() OVER (
            PARTITION BY d.department_name
            ORDER BY salary DESC) AS salary_rank
FROM department d JOIN employee e 
ON d.id = e.department_id
