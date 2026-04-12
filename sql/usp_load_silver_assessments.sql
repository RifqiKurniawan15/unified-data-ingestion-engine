-- PROCEDURE: silver.usp_load_silver_assessments()

-- DROP PROCEDURE IF EXISTS silver.usp_load_silver_assessments();

CREATE OR REPLACE PROCEDURE silver.usp_load_silver_assessments(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    v_error_msg TEXT;
    v_error_detail TEXT;
BEGIN
    -- 1. Ensure the Silver schema table exists with proper types
    CREATE TABLE IF NOT EXISTS silver.assessments (
        assessment_id VARCHAR(50),
        student_id VARCHAR(50),
        subject VARCHAR(100),
        score INT,
        max_score INT,
        assessment_date DATE,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- 2. Clear old data
    TRUNCATE TABLE silver.assessments;

    -- 3. Transformation Process (Casting and Deduplication)
    INSERT INTO silver.assessments (
        assessment_id,
        student_id,
        subject,
        score,
        max_score,
        assessment_date
    )
    SELECT 
        assessment_id,
        student_id,
        subject,
        score::INT,
        max_score::INT,
        assessment_date::DATE
    FROM (
        SELECT 
            assessment_id,
            student_id,
            subject,
            score,
            max_score,
            assessment_date,
            ROW_NUMBER() OVER (
                PARTITION BY assessment_id 
                ORDER BY created_at DESC
            ) as rn
        FROM bronze.stg_assessments
    ) t
    WHERE rn = 1;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT,
                          v_error_detail = PG_EXCEPTION_DETAIL;

    INSERT INTO silver.error_logs (proc_name, error_message, error_detail)
    VALUES ('usp_load_silver_assessments', v_error_msg, v_error_detail);
    
    RAISE EXCEPTION 'Stored Procedure Failed: %', v_error_msg;
END;
$BODY$;

ALTER PROCEDURE silver.usp_load_silver_assessments()
    OWNER TO root;

