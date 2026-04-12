-- PROCEDURE: silver.usp_load_silver_students()

-- DROP PROCEDURE IF EXISTS silver.usp_load_silver_students();

CREATE OR REPLACE PROCEDURE silver.usp_load_silver_students(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    v_error_msg TEXT;
    v_error_detail TEXT;
BEGIN
    -- 1. Ensure the Silver schema table exists
    CREATE TABLE IF NOT EXISTS silver.students (
        student_id VARCHAR(50), 
        student_name VARCHAR(255),
        class_id VARCHAR(50),
        grade_level VARCHAR(20), 
        enrollment_status VARCHAR(50),
        updated_at TIMESTAMP,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- 2. Clear old data (Truncate for full load)
    TRUNCATE TABLE silver.students;

    -- 3. Transformation Process (Deduplication using Row Number)
    INSERT INTO silver.students (
        student_id, 
        student_name, 
        class_id, 
        grade_level, 
        enrollment_status, 
        updated_at
    )
    SELECT 
        student_id, 
        student_name, 
        class_id, 
        grade_level, 
        enrollment_status, 
        updated_at::TIMESTAMP
    FROM (
        SELECT 
            student_id, 
            student_name, 
            class_id, 
            grade_level, 
            enrollment_status, 
            updated_at,
            ROW_NUMBER() OVER (
                PARTITION BY student_id 
                ORDER BY updated_at DESC
            ) as rn
        FROM bronze.stg_students
    ) t
    WHERE rn = 1;

    -- NOTE: No manual COMMIT/ROLLBACK here to avoid subtransaction errors in Airflow

EXCEPTION WHEN OTHERS THEN
    -- Capture error details
    GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT,
                          v_error_detail = PG_EXCEPTION_DETAIL;

    -- Log error into the dedicated error_logs table
    INSERT INTO silver.error_logs (proc_name, error_message, error_detail)
    VALUES ('usp_load_silver_students', v_error_msg, v_error_detail);
    
    -- Raise the exception to notify Airflow that the task failed
    RAISE EXCEPTION 'Stored Procedure Failed: %', v_error_msg;
END;
$BODY$;

ALTER PROCEDURE silver.usp_load_silver_students()
    OWNER TO root;

