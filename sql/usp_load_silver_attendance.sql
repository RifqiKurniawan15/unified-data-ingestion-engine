-- PROCEDURE: silver.usp_load_silver_attendance()

-- DROP PROCEDURE IF EXISTS silver.usp_load_silver_attendance();

CREATE OR REPLACE PROCEDURE silver.usp_load_silver_attendance(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    v_error_msg TEXT;
    v_error_detail TEXT;
BEGIN
    -- 1. Ensure the Silver schema table exists with proper types
    CREATE TABLE IF NOT EXISTS silver.attendance (
        attendance_id VARCHAR(50),
        student_id VARCHAR(50),
        attendance_date DATE,
        status VARCHAR(20),
        created_at TIMESTAMP,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- 2. Clear old data for a fresh load
    TRUNCATE TABLE silver.attendance;

    -- 3. Transformation Process (Casting and Deduplication)
    INSERT INTO silver.attendance (
        attendance_id,
        student_id,
        attendance_date,
        status,
        created_at
    )
    SELECT 
        attendance_id,
        student_id,
        attendance_date::DATE,      -- Cast TEXT to DATE
        status,
        created_at::TIMESTAMP       -- Cast TEXT to TIMESTAMP
    FROM (
        SELECT 
            attendance_id,
            student_id,
            attendance_date,
            status,
            created_at,
            ROW_NUMBER() OVER (
                PARTITION BY attendance_id 
                ORDER BY created_at DESC
            ) as rn
        FROM bronze.stg_attendance
    ) t
    WHERE rn = 1;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT,
                          v_error_detail = PG_EXCEPTION_DETAIL;

    INSERT INTO silver.error_logs (proc_name, error_message, error_detail)
    VALUES ('usp_load_silver_attendance', v_error_msg, v_error_detail);
    
    RAISE EXCEPTION 'Stored Procedure Failed: %', v_error_msg;
END;
$BODY$;

ALTER PROCEDURE silver.usp_load_silver_attendance()
    OWNER TO root;

